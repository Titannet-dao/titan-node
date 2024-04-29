package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"unsafe"

	"github.com/Filecoin-Titan/titan/node/edge/clib"
)

var (
	lib *clib.CLib
)

//export FreeCString
func FreeCString(jsonStrPtr *C.char) {
	C.free(unsafe.Pointer(jsonStrPtr))
}

//export JSONCall
func JSONCall(jsonStrPtr *C.char) *C.char {
	jsonStr := C.GoString(jsonStrPtr)

	if lib == nil {
		lib = clib.NewCLib(daemonStart)
	}

	result := lib.JSONCall(jsonStr)
	resultJson, err := json.Marshal(result)
	if err != nil {
		log.Errorf("marsal result error ", err.Error())
	}

	return C.CString(string(resultJson))
}
