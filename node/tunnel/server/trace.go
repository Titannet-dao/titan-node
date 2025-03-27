package tunserver

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
)

const headerRequestNodes = "Request-Nodes"
const headerRequestNodesTimestamps = "Request-Nodes-Timestamps"
const headerResponseNodes = "Response-Nodes"
const headerResponseNodesTimestamps = "Response-Nodes-Timestamps"

func AppendTraceInfoToRequestHeader(buf []byte, nodeID string) []byte {
	// Check for http headers
	if !strings.Contains(string(buf), "HTTP/1.1") {
		return buf
	}

	headerString := ""
	remainingData := bytes.Buffer{}
	reader := bufio.NewReader(bytes.NewReader(buf))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		if line == "\r\n" {
			_, err = io.Copy(&remainingData, reader)
			if err != nil {
				log.Errorf("AppendTraceInfoToRequestHeader %s", err.Error())
			}
			break
		}
		headerString += line
	}

	headerString += fmt.Sprintf("%s: %s\r\n", headerRequestNodes, nodeID)
	headerString += fmt.Sprintf("%s: %d\r\n", headerRequestNodesTimestamps, time.Now().UnixMilli())

	headerString += "\r\n"
	return append([]byte(headerString), remainingData.Bytes()...)
}

func AppendTraceInfoToResponseHeader(buf []byte, nodeID string) []byte {
	// Check for http headers
	if !strings.Contains(string(buf), "HTTP/1.1") {
		return buf
	}

	headerString := ""
	remainingData := bytes.Buffer{}
	reader := bufio.NewReader(bytes.NewReader(buf))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		if line == "\r\n" {
			_, err = io.Copy(&remainingData, reader)
			if err != nil {
				log.Errorf("AppendTraceInfoToResponseHeader %s", err.Error())
			}
			break
		}

		headerString += line
	}

	headerString += fmt.Sprintf("%s: %s\r\n", headerResponseNodes, nodeID)
	headerString += fmt.Sprintf("%s: %d\r\n", headerResponseNodesTimestamps, time.Now().UnixMilli())

	headerString += "\r\n"
	return append([]byte(headerString), remainingData.Bytes()...)
}
