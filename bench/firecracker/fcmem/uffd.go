package main

// The userfaultfd ioctls a handler of Firecracker's needs (linux/userfaultfd.h); Firecracker
// creates the descriptor, registers guest memory with it in MISSING mode, and hands it over.

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	uffdEventPagefault = 0x12
	uffdEventRemove    = 0x15

	// _IOWR(0xAA, 0x03, struct uffdio_copy), _IOR(0xAA, 0x02, struct uffdio_range) and
	// _IOWR(0xAA, 0x04, struct uffdio_zeropage).
	uffdioCopy     = 0xc028aa03
	uffdioWake     = 0x8010aa02
	uffdioZeropage = 0xc020aa04

	uffdMsgSize = 32
)

type uffdioCopyArg struct {
	dst, src, len, mode uint64
	copied              int64
}

type uffdioRangeArg struct {
	start, len uint64
}

type uffdioZeropageArg struct {
	rng    uffdioRangeArg
	mode   uint64
	zeroed int64
}

// uffdMsg is the part of struct uffd_msg the handler reads: the event and, for a page fault,
// its address; for a remove, its range.
type uffdMsg struct {
	event      uint8
	address    uint64
	start, end uint64
}

func parseMsgs(buf []byte) []uffdMsg {
	msgs := make([]uffdMsg, 0, len(buf)/uffdMsgSize)
	for off := 0; off+uffdMsgSize <= len(buf); off += uffdMsgSize {
		m := buf[off : off+uffdMsgSize]
		msg := uffdMsg{event: m[0]}
		switch msg.event {
		case uffdEventPagefault:
			// struct uffd_msg: event, three reserved fields, then arg.pagefault {flags, address}.
			msg.address = binary.LittleEndian.Uint64(m[16:24])
		case uffdEventRemove:
			msg.start = binary.LittleEndian.Uint64(m[8:16])
			msg.end = binary.LittleEndian.Uint64(m[16:24])
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func ioctl(fd int, req uintptr, arg unsafe.Pointer) error {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), req, uintptr(arg))
	if errno != 0 {
		return errno
	}
	return nil
}

// copyPages installs len(src) bytes at dst, pages of pageSize, and wakes the threads faulting
// there. It goes on where the kernel stopped: while the address space is changing (EAGAIN), and
// past a page already there (EEXIST), whose faults it wakes, so that every page of the range is in
// when it returns.
func copyPages(fd int, dst uintptr, src []byte, pageSize uintptr) error {
	for len(src) > 0 {
		arg := uffdioCopyArg{dst: uint64(dst), src: uint64(uintptr(unsafe.Pointer(&src[0]))), len: uint64(len(src))}
		err := ioctl(fd, uffdioCopy, unsafe.Pointer(&arg))
		if err == nil {
			return nil
		}
		if arg.copied > 0 {
			dst += uintptr(arg.copied)
			src = src[arg.copied:]
		}
		switch {
		case errors.Is(err, unix.EAGAIN):
		case errors.Is(err, unix.EEXIST):
			if err := wake(fd, dst, pageSize); err != nil {
				return err
			}
			dst += pageSize
			src = src[pageSize:]
		default:
			return err
		}
	}
	return nil
}

// zeroPages maps the zero page over length bytes of base pages at dst, as copyPages does a copy.
func zeroPages(fd int, dst, length, pageSize uintptr) error {
	for length > 0 {
		arg := uffdioZeropageArg{rng: uffdioRangeArg{start: uint64(dst), len: uint64(length)}}
		err := ioctl(fd, uffdioZeropage, unsafe.Pointer(&arg))
		if err == nil {
			return nil
		}
		if arg.zeroed > 0 {
			dst += uintptr(arg.zeroed)
			length -= uintptr(arg.zeroed)
		}
		switch {
		case errors.Is(err, unix.EAGAIN):
		case errors.Is(err, unix.EEXIST):
			if err := wake(fd, dst, pageSize); err != nil {
				return err
			}
			dst += pageSize
			length -= pageSize
		default:
			return err
		}
	}
	return nil
}

func wake(fd int, start, length uintptr) error {
	arg := uffdioRangeArg{start: uint64(start), len: uint64(length)}
	return ioctl(fd, uffdioWake, unsafe.Pointer(&arg))
}
