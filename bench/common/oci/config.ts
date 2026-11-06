/**
 * The OCI runtime spec the runc-based systems (gvisor, criu) run the instance with, as
 * DESIGN.md section 2 says: alasio's session image's root filesystem, read-only; bayma as
 * alasio's pool pods run it, as `agent`; /attach bound from the run's own directory; /tmp a
 * tmpfs; 2 CPUs and 5 GiB; the network namespace given. runc's default capabilities and mounts
 * otherwise, as `runc spec` writes them.
 *
 *   node config.ts --rootfs <dir> --attach <dir> --netns <path> [--port 8765] [--cgroups-path <path>] > config.json
 */
import { parseArgs } from "node:util";

/** The instance's command, as alasio's pool pods run it (cli/src/manifests/sandbox-gateway.ts). */
export const command = (port: number): string[] => [
  "/usr/bin/tini", "-s", "--", "/usr/local/bin/alasio-start", "node", "/opt/bayma/bayma.js",
  "mcp-http", "--host", "0.0.0.0", "--port", String(port), "--state-dir", "/home/agent/.bayma", "--token-file", "/attach/token",
];

/** bayma's hook, as alasio's gateway gives runsc it (src/gateway/snapshots.ts hookOf). */
export const HOOK = ["/usr/local/bin/alasio-start", "node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"];

/** runc spec's default capabilities. */
const CAPABILITIES = ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"];

export function config({ rootfs, attach, netns, port = 8765, cgroupsPath }: { readonly rootfs: string; readonly attach: string; readonly netns: string; readonly port?: number; readonly cgroupsPath?: string }) {
  return {
    ociVersion: "1.2.0",
    process: {
      terminal: false,
      user: { uid: 1000, gid: 1000 },
      args: command(port),
      env: [
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME=/home/agent",
        "USER=agent",
        "BAYMA_PAYLOAD_DIR=/opt/bayma/payload",
      ],
      cwd: "/attach",
      capabilities: { bounding: CAPABILITIES, effective: CAPABILITIES, permitted: CAPABILITIES },
      rlimits: [{ type: "RLIMIT_NOFILE", hard: 1048576, soft: 1048576 }],
      noNewPrivileges: true,
    },
    root: { path: rootfs, readonly: true },
    hostname: "bench",
    mounts: [
      { destination: "/proc", type: "proc", source: "proc" },
      { destination: "/dev", type: "tmpfs", source: "tmpfs", options: ["nosuid", "strictatime", "mode=755", "size=65536k"] },
      { destination: "/dev/pts", type: "devpts", source: "devpts", options: ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"] },
      { destination: "/dev/shm", type: "tmpfs", source: "shm", options: ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"] },
      { destination: "/dev/mqueue", type: "mqueue", source: "mqueue", options: ["nosuid", "noexec", "nodev"] },
      { destination: "/sys", type: "sysfs", source: "sysfs", options: ["nosuid", "noexec", "nodev", "ro"] },
      { destination: "/sys/fs/cgroup", type: "cgroup", source: "cgroup", options: ["nosuid", "noexec", "nodev", "relatime", "ro"] },
      { destination: "/tmp", type: "tmpfs", source: "tmpfs", options: ["nosuid", "nodev", "mode=1777"] },
      { destination: "/attach", type: "bind", source: attach, options: ["rbind", "rw"] },
    ],
    linux: {
      // Where the harness reads the instance's memory.peak and memory.current, where it names one.
      ...(cgroupsPath === undefined ? {} : { cgroupsPath }),
      resources: {
        memory: { limit: 5 * 1024 ** 3, swap: 5 * 1024 ** 3 },
        cpu: { quota: 200_000, period: 100_000 },
      },
      namespaces: [{ type: "pid" }, { type: "network", path: netns }, { type: "ipc" }, { type: "uts" }, { type: "mount" }, { type: "cgroup" }],
      maskedPaths: ["/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys", "/proc/latency_stats", "/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/sys/firmware", "/proc/scsi"],
      readonlyPaths: ["/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"],
    },
  };
}

// Run as a program, not imported, nor bundled into another's (../driver/build.ts).
if (/(^|\/)config\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  const { values } = parseArgs({ options: { rootfs: { type: "string" }, attach: { type: "string" }, netns: { type: "string" }, port: { type: "string", default: "8765" }, "cgroups-path": { type: "string" } } });
  if (!values.rootfs || !values.attach || !values.netns) throw new Error("usage: config.ts --rootfs <dir> --attach <dir> --netns <path> [--port 8765]");
  const cgroupsPath = values["cgroups-path"];
  console.log(JSON.stringify(config({ rootfs: values.rootfs, attach: values.attach, netns: values.netns, port: Number(values.port), ...(cgroupsPath === undefined ? {} : { cgroupsPath }) }), null, 2));
}
