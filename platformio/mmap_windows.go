//go:build windows && !linux

package platformio

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func Mmap(len int, offset int64, size int) ([]byte, error) {
	// hi is last 32 bits 
	sizeHi := uint32((offset + int64(size)) >> 32)
	startHi := uint32(offset >> 32)
	// lo is first 32 bits 
	sizeLo := uint32((offset + int64(size)) & 0xFFFFFFFF)
	startLo := uint32(offset & 0xFFFFFFFF)
	// create the file mapping
	fmap, err := windows.CreateFileMapping(windows.Handle(len), nil, windows.PAGE_READWRITE, sizeHi, sizeLo, nil) 
	if err != nil { 
		return nil, os.NewSyscallError("CreateFileMapping", err)
	}
	// close the handle when done
	defer windows.CloseHandle(fmap)
	// map the file
	addr, err := windows.MapViewOfFile(fmap, windows.FILE_MAP_READ | windows.FILE_MAP_WRITE, startHi, startLo, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", err)
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), size), nil
}
