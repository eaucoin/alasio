// The guest's PID 1: it makes the microVM what the runc-based systems' container is to bayma
// (bench/common/oci/config.ts), then runs alasio's pool pod's command in it. The root drive, the
// session image's filesystem, is mounted read-only by the kernel and shared by every run; the
// run's /attach disk is mounted where the container binds the run's directory; /tmp is a tmpfs,
// as the container's is, so its files are guest memory as theirs are the container's. The
// network is the kernel's (`ip=` on its command line) but for loopback, which Linux leaves
// down. Then the command, as user agent, from /attach, with the container's environment,
// hostname, open-file limit and no new privileges.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

// The run's /attach disk, the drive the harness attaches after the root drive.
const attachDisk = "/dev/vdb"

// The instance's command and environment (bench/common/oci/config.ts, after
// cli/src/manifests/sandbox-gateway.ts).
var (
	command = []string{
		"/usr/bin/tini", "-s", "--", "/usr/local/bin/alasio-start", "node", "/opt/bayma/bayma.js",
		"mcp-http", "--host", "0.0.0.0", "--port", "8765", "--state-dir", "/home/agent/.bayma",
		"--token-file", "/attach/token",
	}
	environment = []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=/home/agent", "USER=agent", "BAYMA_PAYLOAD_DIR=/opt/bayma/payload",
	}
)

const (
	agentID   = 1000
	openFiles = 1 << 20
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bench-init:", err)
	}
	// PID 1 ending panics the kernel; with `panic=1 reboot=k` that ends the microVM.
	os.Exit(1)
}

func run() error {
	const nosuidNodevNoexec = unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC
	mounts := []struct {
		source, target, fstype string
		flags                  uintptr
		data                   string
	}{
		{"proc", "/proc", "proc", nosuidNodevNoexec, ""},
		{"sysfs", "/sys", "sysfs", nosuidNodevNoexec | unix.MS_RDONLY, ""},
		{"cgroup2", "/sys/fs/cgroup", "cgroup2", nosuidNodevNoexec | unix.MS_RDONLY, ""},
		{"devpts", "/dev/pts", "devpts", unix.MS_NOSUID | unix.MS_NOEXEC, "newinstance,ptmxmode=0666,mode=0620,gid=5"},
		{"shm", "/dev/shm", "tmpfs", nosuidNodevNoexec, "mode=1777,size=65536k"},
		{"mqueue", "/dev/mqueue", "mqueue", nosuidNodevNoexec, ""},
		{"tmpfs", "/tmp", "tmpfs", unix.MS_NOSUID | unix.MS_NODEV, "mode=1777"},
		{attachDisk, "/attach", "ext4", 0, ""},
	}
	for _, m := range mounts {
		// Those under /dev, which the kernel mounts a devtmpfs on, are made; the rest are the image's.
		if err := os.MkdirAll(m.target, 0o755); err != nil {
			return err
		}
		if err := unix.Mount(m.source, m.target, m.fstype, m.flags, m.data); err != nil {
			return fmt.Errorf("mount %s on %s: %w", m.source, m.target, err)
		}
	}
	// The links a container runtime makes in /dev.
	for link, target := range map[string]string{"/dev/fd": "/proc/self/fd", "/dev/stdin": "/proc/self/fd/0", "/dev/stdout": "/proc/self/fd/1", "/dev/stderr": "/proc/self/fd/2", "/dev/ptmx": "pts/ptmx"} {
		os.Remove(link)
		if err := os.Symlink(target, link); err != nil {
			return err
		}
	}
	if err := unix.Sethostname([]byte("bench")); err != nil {
		return err
	}
	if err := loopbackUp(); err != nil {
		return err
	}
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: openFiles, Max: openFiles}); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Dir = "/attach"
	cmd.Env = environment
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: agentID, Gid: agentID, Groups: []uint32{}},
		Setsid:     true,
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	fmt.Println("bench-init: started bayma")
	// Reap whatever is orphaned to PID 1 until bayma's tini ends.
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, 0, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		if pid == cmd.Process.Pid {
			return fmt.Errorf("bayma ended: %v", status)
		}
	}
}

// loopbackUp sets lo up, as a container runtime does in the container's network namespace.
func loopbackUp() error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	ifr, err := unix.NewIfreq("lo")
	if err != nil {
		return err
	}
	if err := unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, ifr); err != nil {
		return err
	}
	ifr.SetUint16(ifr.Uint16() | unix.IFF_UP)
	return unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, ifr)
}
