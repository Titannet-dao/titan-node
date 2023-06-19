package lotuscli

import (
	"encoding/json"
	"errors"
)

type request struct {
	Jsonrpc string     `json:"jsonrpc"`
	Method  string     `json:"method"`
	Params  rawMessage `json:"params"`
	ID      int        `json:"id"`
}

type rawMessage []byte

// MarshalJSON returns m as the JSON encoding of m.
func (m rawMessage) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

// Response defines a JSON RPC response from the spec
// http://www.jsonrpc.org/specification#response_object
type response struct {
	Jsonrpc string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	ID      interface{} `json:"id"`
	Error   *respError  `json:"error,omitempty"`
}

type respError struct {
	Code    errorCode       `json:"code"`
	Message string          `json:"message"`
	Meta    json.RawMessage `json:"meta,omitempty"`
}

type errorCode int

// UnmarshalJSON sets *m to a copy of data.
func (m *rawMessage) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("json.RawMessage: UnmarshalJSON on nil pointer")
	}
	*m = append((*m)[0:0], data...)
	return nil
}

type params []interface{}

type (
	// lotus struct
	tipSet struct {
		Height int64
	}

	randomness []byte
)
