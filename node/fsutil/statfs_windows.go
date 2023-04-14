package fsutil

import (
	"syscall"
	"unsafe"
)

func Statfs(volumePath string) (FsStat, error) {
	// From https://github.com/ricochet2200/go-disk-usage/blob/master/du/diskusage_windows.go

	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")

	var freeBytes int64
	var totalBytes int64
	var availBytes int64

	ptr, err := syscall.UTF16PtrFromString(volumePath)
	if err != nil {
		return FsStat{}, err
	}

	_, _, err = c.Call(
		uintptr(unsafe.Pointer(ptr)),         //nolint:gosec // ignore error
		uintptr(unsafe.Pointer(&freeBytes)),  //nolint:gosec // ignore error
		uintptr(unsafe.Pointer(&totalBytes)), //nolint:gosec // ignore error
		uintptr(unsafe.Pointer(&availBytes))) //nolint:gosec // ignore error

	if err != nil {
		return FsStat{}, err
	}

	return FsStat{
		Capacity:    totalBytes,
		Available:   availBytes,
		FSAvailable: availBytes,
	}, nil
}
