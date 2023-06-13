//go:build linux && !windows
// +build linux,!windows

package platformio

import (
	"golang.org/x/sys/unix"
	"os"
)

func Mmap(len int, offset int64, size int) ([]byte, error) {
	slice, err := unix.Mmap(len, offset, size, unix.PROT_READ_WRTIE, unix.MAP_SHARED)
	if err != nil {
		return nil, os.NewSyscallError("Mmap", err)
	}
	return slice, nil
}

func Munmap(chunk []byte) {
	return unix.Munmap(chunk)
}
