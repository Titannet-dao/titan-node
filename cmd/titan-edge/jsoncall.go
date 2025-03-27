package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"encoding/json"
	"os"
	"sync"
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

	log.Infoln("JSONCall Req: ", string(jsonStr))

	if lib == nil {
		lib = clib.NewCLib(daemonStart)
	}

	result := lib.JSONCall(jsonStr)
	resultJson, err := json.Marshal(result)
	log.Infoln("JSONCall Resp: ", result)

	if err != nil {
		log.Errorf("marsal result error ", err.Error())
	}

	return C.CString(string(resultJson))
}

func daemonStart(ctx context.Context, daemonSwitch *clib.DaemonSwitch, repoPath, locatorURL string) error {
	// TODO： Set this environment variable only for individual systems
	err := os.Setenv("QUIC_GO_DISABLE_ECN", "true")
	if err != nil {
		log.Errorf("Error setting environment QUIC_GO_DISABLE_ECN:", err)
	}

	ok, err := registerNodeIfNotExist(repoPath, locatorURL)
	if err != nil {
		return err
	}

	if ok {
		log.Infof("daemonStart register new node")
	}

	d, err := newDaemon(ctx, repoPath, daemonSwitch)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go d.startServer(wg)

	wg.Add(1)
	go d.connectToServer(wg)

	go d.waitShutdown(wg)

	return nil
}
