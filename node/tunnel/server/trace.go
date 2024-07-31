package tunserver

import (
	"bufio"
	"bytes"
	"fmt"
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
	reader := bufio.NewReader(bytes.NewReader(buf))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		if line == "\r\n" {
			break
		}
		headerString += line
	}

	headerString += fmt.Sprintf("%s: %s\r\n", headerRequestNodes, nodeID)
	headerString += fmt.Sprintf("%s: %d\r\n", headerRequestNodesTimestamps, time.Now().UnixMilli())

	headerString += "\r\n"
	return []byte(headerString)
}

func AppendTraceInfoToResponseHeader(buf []byte, nodeID string) []byte {
	// Check for http headers
	if !strings.Contains(string(buf), "HTTP/1.1") {
		return buf
	}

	headerString := ""
	reader := bufio.NewReader(bytes.NewReader(buf))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		if line == "\r\n" {
			break
		}

		headerString += line
	}

	headerString += fmt.Sprintf("%s: %s\r\n", headerResponseNodes, nodeID)
	headerString += fmt.Sprintf("%s: %d\r\n", headerResponseNodesTimestamps, time.Now().UnixMilli())

	headerString += "\r\n"
	return []byte(headerString)
}
