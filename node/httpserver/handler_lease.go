package httpserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/gbrlsnchs/jwt/v3"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	cliutil "github.com/Filecoin-Titan/titan/cli/util"
	"github.com/gorilla/websocket"
	"k8s.io/client-go/tools/remotecommand"
)

var (
	pingPeriod = time.Second * 10
	pingWait   = time.Second * 15
)

type terminalSizeQueue struct {
	resize chan remotecommand.TerminalSize
}

func (w *terminalSizeQueue) Next() *remotecommand.TerminalSize {
	ret, ok := <-w.resize
	if !ok {
		return nil
	}
	return &ret
}

func (hs *HttpServer) LeaseShellHandler(writer http.ResponseWriter, req *http.Request) {
	// POST /lease/<lease-id>/shell
	paths := strings.Split(req.URL.Path, "/")
	if len(paths) < 3 {
		uploadResult(writer, -1, fmt.Sprintf("%s, http status code %d", "invalid request", http.StatusBadRequest))
		return
	}

	id := paths[2]

	payload, err := hs.verifyLeaseToken(req)
	if err != nil {
		log.Errorf("verfiy token error: %s", err.Error())
		uploadResult(writer, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusUnauthorized))
		return
	}

	deployments, err := hs.scheduler.GetDeploymentList(context.Background(), &types.GetDeploymentOption{DeploymentID: types.DeploymentID(payload.DeploymentID)})
	if err != nil {
		uploadResult(writer, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusUnauthorized))
		return
	}

	if len(deployments.Deployments) <= 0 || deployments.Deployments[0].Owner != payload.UserID {
		uploadResult(writer, -1, fmt.Sprintf("%s, http status code %d", "invalid token", http.StatusUnauthorized))
		return
	}

	fmt.Println("connection from: ", payload.UserID, req.URL.String(), id)

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	params := req.URL.Query()
	tty := params.Get("tty")
	isTty := tty == "1"

	stdin := params.Get("stdin")
	connectStdin := stdin == "1"

	conn, err := upgrader.Upgrade(writer, req, nil)
	if err != nil {
		log.Errorf("upgrader %v", err)
		return
	}

	fmt.Println("connection coming")

	var command []string

	for i := 0; true; i++ {
		v := params.Get(fmt.Sprintf("cmd%d", i))
		if 0 == len(v) {
			break
		}
		command = append(command, v)
	}

	podName := params.Get("pod")

	var stdinPipeOut *io.PipeWriter
	var stdinPipeIn *io.PipeReader
	wg := &sync.WaitGroup{}

	var tsq remotecommand.TerminalSizeQueue
	var terminalSizeUpdate chan remotecommand.TerminalSize
	if isTty {
		terminalSizeUpdate = make(chan remotecommand.TerminalSize, 1)
		tsq = &terminalSizeQueue{resize: terminalSizeUpdate}
	}

	if connectStdin {
		stdinPipeIn, stdinPipeOut = io.Pipe()
		wg.Add(1)
		go websocketHandler(wg, conn, stdinPipeOut, terminalSizeUpdate)
	}

	l := &sync.Mutex{}
	stdout := cliutil.NewWsWriterWrapper(conn, types.ShellCodeStdout, l)
	stderr := cliutil.NewWsWriterWrapper(conn, types.ShellCodeStderr, l)

	subctx, subcancel := context.WithCancel(req.Context())
	wg.Add(1)
	go leaseShellPingHandler(subctx, wg, conn)

	var stdinForExec io.ReadCloser
	if connectStdin {
		stdinForExec = stdinPipeIn
	}

	result, err := hs.client.Exec(subctx, types.DeploymentID(id), podName, stdinForExec, stdout, stderr, command, isTty, tsq)
	if err != nil {
		log.Errorf("exec: %v", err)
	}

	subcancel()

	responseData := types.ShellResponse{}
	var resultWriter io.Writer
	encodeData := true
	resultWriter = cliutil.NewWsWriterWrapper(conn, types.ShellCodeResult, l)

	responseData.ExitCode = result.Code

	if err != nil {
		responseData.Message = err.Error()
	}

	if encodeData {
		encoder := json.NewEncoder(resultWriter)
		err = encoder.Encode(responseData)
	} else {
		_, err = resultWriter.Write([]byte{})
	}

	_ = conn.Close()

	wg.Wait()

	if stdinPipeIn != nil {
		_ = stdinPipeIn.Close()
	}

	if stdinPipeOut != nil {
		_ = stdinPipeOut.Close()
	}

	if terminalSizeUpdate != nil {
		close(terminalSizeUpdate)
	}

}

func leaseShellPingHandler(ctx context.Context, wg *sync.WaitGroup, ws *websocket.Conn) {
	defer wg.Done()
	pingTicker := time.NewTicker(pingPeriod)
	defer pingTicker.Stop()

	for {
		select {
		case <-pingTicker.C:
			const pingWriteWaitTime = 5 * time.Second
			if err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(pingWriteWaitTime)); err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func websocketHandler(wg *sync.WaitGroup, conn *websocket.Conn, stdout io.WriteCloser, terminalSizeUpdate chan<- remotecommand.TerminalSize) {
	defer wg.Done()

	for {
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pingWait))
		})

		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if messageType != websocket.BinaryMessage || len(data) == 0 {
			continue
		}

		msgID := data[0]
		msg := data[1:]

		switch msgID {
		case types.ShellCodeStdin:
			_, err := stdout.Write(msg)
			if err != nil {
				return
			}
		case types.ShellCodeEOF:
			err = stdout.Close()
			if err != nil {
				return
			}
		case types.ShellCodeTerminalResize:
			var size remotecommand.TerminalSize
			r := bytes.NewReader(msg)
			// Unpack data, its just binary encoded data in big endian
			err = binary.Read(r, binary.BigEndian, &size.Width)
			if err != nil {
				return
			}
			err = binary.Read(r, binary.BigEndian, &size.Height)
			if err != nil {
				return
			}

			log.Info("terminal resize received", "width", size.Width, "height", size.Height)
			if terminalSizeUpdate != nil {
				terminalSizeUpdate <- size
			}
		default:
			log.Error("unknown message ID on websocket", "code", msgID)
			return
		}
	}
}

func (hs *HttpServer) verifyLeaseToken(r *http.Request) (*types.AuthUserDeployment, error) {
	token := r.Header.Get("Authorization")
	if token == "" {
		token = r.FormValue("token")
		if token != "" {
			token = "Bearer " + token
		}
	}

	if token == "" {
		token = r.URL.Query().Get("token")
		if token != "" {
			token = "Bearer " + token
		}
	}

	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			return nil, fmt.Errorf("missing Bearer prefix in auth header")
		}
		token = strings.TrimPrefix(token, "Bearer ")
	}

	payload := &types.AuthUserDeployment{}
	if _, err := jwt.Verify([]byte(token), hs.apiSecret, payload); err != nil {
		return nil, err
	}

	if time.Now().After(payload.Expiration) {
		return nil, fmt.Errorf("token is expire, userID %s, deploymentID:%s", payload.UserID, payload.DeploymentID)
	}

	return payload, nil
}
