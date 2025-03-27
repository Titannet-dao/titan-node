package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Filecoin-Titan/titan/node/edge/clib"
	"github.com/urfave/cli/v2"
)

var daemonTestCmd = &cli.Command{
	Name:  "test",
	Usage: "Daemon test commands",
	Subcommands: []*cli.Command{
		testDaemonStartCmd,
		testSignCmd,
	},
}

var testDaemonStartCmd = &cli.Command{
	Name:  "start",
	Usage: "test a running daemon",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "url",
			Usage: "--url=https://titan-server-domain/rpc/v0",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		ret, err := testStartDaemon()
		if err != nil {
			return err
		}

		fmt.Println("ret ", ret)
		go testGetStat()

		// 阻塞主 goroutine，直到收到中断信号
		fmt.Println("wait for term signal ...")

		// go func() {
		// 	time.Sleep(3 * time.Second)
		// 	testDownloadFile()
		// }()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		for {
			sig := <-sigCh

			switch sig {
			case syscall.SIGINT:
				// testDownloadCancel()
				testStopDaemon()
				// time.Sleep(10 * time.Second)
				fmt.Println("Received SIGINT (Ctrl+C)")
				// return nil
			case syscall.SIGTERM:
				testStopDaemon()
				time.Sleep(10 * time.Second)
				fmt.Println("Received SIGTERM")
				return nil
				// testDownloadCancel()
			case syscall.SIGQUIT:
				fmt.Println("Received SIGQUIT")
				// 在这里执行相应的处理逻辑
			}
		}

	},
}

func testStartDaemon() (string, error) {
	args := clib.StartDaemonReq{RepoPath: "C:/Users/aaa/.titanedge", LocatorURL: "https://cassini-locator.titannet.io:5000/rpc/v0"}
	startDaemonArgsJSON, err := json.Marshal(args)
	if err != nil {
		fmt.Println("err ", err.Error())
		return "", err
	}

	jsonCallArgs := clib.JSONCallArgs{Method: "startDaemon", JSONParams: string(startDaemonArgsJSON)}
	jsonCallArgsJSON, err := json.Marshal(jsonCallArgs)
	if err != nil {
		fmt.Println("err ", err.Error())
		return "", err
	}

	resultJsonPtr := C.CString(string(jsonCallArgsJSON))
	ret := JSONCall(resultJsonPtr)

	retStr := C.GoString(ret)
	FreeCString(resultJsonPtr)
	FreeCString(ret)

	return retStr, nil
}

func testStopDaemon() {
	jsonCallArgs := clib.JSONCallArgs{Method: "stopDaemon", JSONParams: ""}
	jsonCallArgsJSON, err := json.Marshal(jsonCallArgs)
	if err != nil {
		fmt.Println("err ", err.Error())
		return
	}

	resultJsonPtr := C.CString(string(jsonCallArgsJSON))
	ret := JSONCall(resultJsonPtr)
	retString := C.GoString(ret)

	FreeCString(resultJsonPtr)
	FreeCString(ret)
	fmt.Println("stopDaemon ", retString)

}

func testGetStat() {
	for {
		jsonCallArgs := clib.JSONCallArgs{Method: "state", JSONParams: ""}
		jsonCallArgsJSON, err := json.Marshal(jsonCallArgs)
		if err != nil {
			fmt.Println("err ", err.Error())
			return
		}

		resultJsonPtr := C.CString(string(jsonCallArgsJSON))
		ret := JSONCall(resultJsonPtr)
		retString := C.GoString(ret)

		FreeCString(resultJsonPtr)
		FreeCString(ret)
		fmt.Println("state ", retString)

		time.Sleep(5 * time.Second)
	}
}

func testDownloadFile() {
	type DownloadFileReq struct {
		CID          string `json:"cid"`
		DownloadPath string `json:"download_path"`
		LocatorURL   string `json:"locator_url"`
	}

	req := DownloadFileReq{CID: "bafybeifaxtfwqqqoxzjvpfra3rsrq46v4vkl7kjoclc7mv4pjy7y5asv24", DownloadPath: "d:/filecoin-titan/test2/830_2533.pdf", LocatorURL: "https://test-locator.titannet.io:5000/rpc/v0"}
	buf, err := json.Marshal(req)
	if err != nil {
		fmt.Println("marshal error ", err.Error())
	}

	jsonCallArgs := clib.JSONCallArgs{Method: "downloadFile", JSONParams: string(buf)}
	jsonCallArgsJSON, err := json.Marshal(jsonCallArgs)
	if err != nil {
		fmt.Println("err ", err.Error())
		return
	}

	resultJsonPtr := C.CString(string(jsonCallArgsJSON))
	ret := JSONCall(resultJsonPtr)
	retString := C.GoString(ret)

	FreeCString(resultJsonPtr)
	FreeCString(ret)
	fmt.Println("state ", retString)

	go testDownloadProgress()
}

func testDownloadCancel() {
	type DownloadCancelReq struct {
		FilePath string `json:"file_path"`
	}
	req := DownloadCancelReq{FilePath: "d:/filecoin-titan/test2/830_2533.pdf"}
	buf, err := json.Marshal(req)
	if err != nil {
		fmt.Println("marshal error ", err.Error())
	}

	jsonCallArgs := clib.JSONCallArgs{Method: "downloadCancel", JSONParams: string(buf)}
	jsonCallArgsJSON, err := json.Marshal(jsonCallArgs)
	if err != nil {
		fmt.Println("err ", err.Error())
		return
	}

	resultJsonPtr := C.CString(string(jsonCallArgsJSON))
	ret := JSONCall(resultJsonPtr)
	retString := C.GoString(ret)

	FreeCString(resultJsonPtr)
	FreeCString(ret)
	fmt.Println("download cancel ", retString)
}

func testDownloadProgress() {
	for {
		type DownloadProgressReq struct {
			FilePath string `json:"file_path"`
		}
		req := DownloadProgressReq{FilePath: "d:/filecoin-titan/test2/830_2533.pdf"}
		buf, err := json.Marshal(req)
		if err != nil {
			fmt.Println("marshal error ", err.Error())
		}

		jsonCallArgs := clib.JSONCallArgs{Method: "downloadProgress", JSONParams: string(buf)}
		jsonCallArgsJSON, err := json.Marshal(jsonCallArgs)
		if err != nil {
			fmt.Println("err ", err.Error())
			return
		}

		resultJsonPtr := C.CString(string(jsonCallArgsJSON))
		ret := JSONCall(resultJsonPtr)
		retString := C.GoString(ret)

		FreeCString(resultJsonPtr)
		FreeCString(ret)
		fmt.Println("progress ", retString)

		time.Sleep(3 * time.Second)
	}

}

var testSignCmd = &cli.Command{
	Name:  "sign",
	Usage: "get daemon state",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		hash := cctx.Args().First()

		req := clib.SignReq{RepoPath: "./test2/titanl2", Hash: hash}
		buf, err := json.Marshal(req)
		if err != nil {
			return err
		}

		jsonCallArgs := clib.JSONCallArgs{Method: "sign", JSONParams: string(buf)}
		jsonCallArgsJSON, err := json.Marshal(jsonCallArgs)
		if err != nil {
			return err
		}

		resultJsonPtr := C.CString(string(jsonCallArgsJSON))
		ret := JSONCall(resultJsonPtr)
		retString := C.GoString(ret)

		FreeCString(resultJsonPtr)
		FreeCString(ret)

		fmt.Println("sign ", retString)

		return nil
	},
}
