package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/Filecoin-Titan/titan/api/types"
	cliutil "github.com/Filecoin-Titan/titan/cli/util"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"io"
	"k8s.io/client-go/tools/remotecommand"
	"net/http"
	"os"
	"sync"
	"time"
)

type ClientResponseError struct {
	Status  int
	Message string
}

func (err ClientResponseError) Error() string {
	return fmt.Sprintf("remote server returned %d", err.Status)
}

func (err ClientResponseError) ClientError() string {
	return fmt.Sprintf("Remote Server returned %d\n%s", err.Status, err.Message)
}

func showErrorToUser(err error) error {
	// If the error has a complete message associated with it then show it
	clientResponseError, ok := err.(ClientResponseError)
	if ok && 0 != len(clientResponseError.Message) {
		_, _ = fmt.Fprintf(os.Stderr, "provider error messsage:\n%v\n", clientResponseError.Message)
		err = nil
	}

	return err
}

func doLeaseShell(ctx context.Context, connUrl string, header http.Header,
	stdin io.ReadCloser,
	stdout io.Writer,
	stderr io.Writer,
	tty bool,
	terminalResize <-chan remotecommand.TerminalSize) error {

	wsclient := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}

	subctx, subcancel := context.WithCancel(ctx)

	conn, response, err := wsclient.DialContext(subctx, connUrl, header)
	if err != nil {
		if errors.Is(err, websocket.ErrBadHandshake) {
			buf := &bytes.Buffer{}
			_, _ = io.Copy(buf, response.Body)

			subcancel()
			return ClientResponseError{
				Status:  response.StatusCode,
				Message: buf.String(),
			}
		}
		subcancel()
		return err
	}

	wg := &sync.WaitGroup{}
	suberr := make(chan error, 1)
	saveError := func(msg string, err error) {
		err = fmt.Errorf("%w: failed while %s", err, msg)
		// The channel is buffered but do not block here
		select {
		case suberr <- err:
		default:
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-subctx.Done()
		err := conn.Close()
		if err != nil {
			saveError("closing websocket", err)
		}
	}()

	l := &sync.Mutex{}

	if stdin != nil {
		stdinWriter := cliutil.NewWsWriterWrapper(conn, types.ShellCodeStdin, l)
		eofWriter := cliutil.NewWsWriterWrapper(conn, types.ShellCodeEOF, l)
		// This goroutine is orphaned. There is no universal way to cancel a read from stdin
		// at this time
		go handleStdin(subctx, stdin, stdinWriter, eofWriter, saveError)
	}

	if tty && terminalResize != nil {
		wg.Add(1)
		terminalOutput := cliutil.NewWsWriterWrapper(conn, types.ShellCodeTerminalResize, l)
		go handleTerminalResize(subctx, wg, terminalResize, terminalOutput, saveError)
	}

	var remoteErrorData *bytes.Buffer
	var connectionError error
loop:
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			saveError("receiving from websocket", err)
			break
		}

		if messageType != websocket.BinaryMessage {
			continue // Just ignore anything else
		}

		if len(data) == 0 {
			connectionError = fmt.Errorf("provider sent a message that is too short to parse, errShell")
		}

		msgID := data[0] // First byte is always message ID
		msg := data[1:]  // remainder is the message

		switch msgID {
		case types.ShellCodeStdout:
			_, connectionError = stdout.Write(msg)
		case types.ShellCodeStderr:
			_, connectionError = stderr.Write(msg)
		case types.ShellCodeResult:
			remoteErrorData = bytes.NewBuffer(msg)
			break loop
		case types.ShellCodeFailure:
			connectionError = errors.New("ErrShellProviderError")
		default:
			connectionError = fmt.Errorf("provider sent unknown message ID %d, %s", messageType, string(msg))
		}

		if connectionError != nil {
			break loop
		}
	}

	subcancel()

	if stdin != nil {
		err := stdin.Close()
		if err != nil {
			saveError("closing stdin", err)
		}
	}

	// Check to see if the remote end returned an error
	if remoteErrorData != nil {
		return processRemoteError(remoteErrorData)
	}

	// Check to see if a goroutine failed
	select {
	case err := <-suberr:
		return err
	default:
	}

	wg.Wait()

	return connectionError
}

func handleStdin(ctx context.Context, input io.Reader, output, eofWriter io.Writer, saveError func(string, error)) {
	data := make([]byte, 4096)

	for {
		n, err := input.Read(data)
		if err == io.EOF {
			eofWriter.Write([]byte(err.Error()))
			return
		}

		if err != nil {
			saveError("reading from stdin", err)
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		_, err = output.Write(data[0:n])
		if err != nil {
			saveError("writing stdin data to remote", err)
			return
		}
	}
}

func handleTerminalResize(ctx context.Context, wg *sync.WaitGroup, input <-chan remotecommand.TerminalSize, output io.Writer, saveError func(string, error)) {
	defer wg.Done()

	buf := &bytes.Buffer{}
	for {
		var size remotecommand.TerminalSize
		var ok bool
		select {
		case <-ctx.Done():
			return
		case size, ok = <-input:
			if !ok { // Channel has closed
				return
			}

		}

		// Clear the buffer, then pack in both values
		buf.Reset()
		err := binary.Write(buf, binary.BigEndian, size.Width)
		if err != nil {
			saveError("encoding doLeaseShell size width", err)
			return
		}
		err = binary.Write(buf, binary.BigEndian, size.Height)
		if err != nil {
			saveError("encoding doLeaseShell size height", err)
			return
		}

		_, err = output.Write((buf).Bytes())
		if err != nil {
			saveError("sending doLeaseShell size to remote", err)
			return
		}
	}
}
func processRemoteError(input io.Reader) error {
	dec := json.NewDecoder(input)
	var v types.ShellResponse
	err := dec.Decode(&v)
	if err != nil {
		return fmt.Errorf("%w: failed parsing response data from provider", err)
	}

	if 0 != len(v.Message) {
		return fmt.Errorf("%w: %s", errors.New("shell failed"), v.Message)
	}

	if 0 != v.ExitCode {
		return fmt.Errorf("%w: remote process exited with code %d", errors.New("shell failed"), v.ExitCode)
	}

	return nil
}
