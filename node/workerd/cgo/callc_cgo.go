//go:build edge
// +build edge

package cgo

/*
#include <stdlib.h>
#cgo LDFLAGS: -lgoworkerd
extern const char* workerdGoRuntimeJsonCall(const char* jsonString);
extern void workerdGoFreeHeapStrPtr(const char* ptr);
*/
import "C"
import (
	"unsafe"
)

func callC(inputs string) (string, error) {
	ptrInputs := C.CString(inputs)
	ptrOutputs := C.workerdGoRuntimeJsonCall(ptrInputs)
	C.free(unsafe.Pointer(ptrInputs))

	strOutputs := C.GoString(ptrOutputs)
	C.workerdGoFreeHeapStrPtr(ptrOutputs)

	return strOutputs, nil
}
