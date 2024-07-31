package cliutil

import (
	"bytes"
	"github.com/gorilla/websocket"
	"io"
	"sync"
)

// This type exposes the single method that this wrapper uses
type wrappedWriterConnection interface {
	WriteMessage(int, []byte) error
}

type wsWriterWrapper struct {
	connection wrappedWriterConnection

	id  byte
	buf bytes.Buffer
	l   sync.Locker
}

func NewWsWriterWrapper(conn wrappedWriterConnection, id byte, l sync.Locker) io.Writer {
	return &wsWriterWrapper{
		connection: conn,
		l:          l,
		id:         id,
	}
}

func (wsw *wsWriterWrapper) Write(data []byte) (int, error) {
	myBuf := &wsw.buf
	myBuf.Reset()
	_ = myBuf.WriteByte(wsw.id)
	_, _ = myBuf.Write(data)

	wsw.l.Lock()
	defer wsw.l.Unlock()
	err := wsw.connection.WriteMessage(websocket.BinaryMessage, myBuf.Bytes())

	return len(data), err
}
