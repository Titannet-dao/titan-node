package api

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/filecoin-project/go-jsonrpc"
)

const (
	EUnknown = iota + jsonrpc.FirstUserCode
	EWeb
)

type ErrUnknown struct{}

func (eu *ErrUnknown) Error() string {
	return "unknown"
}

type ErrWeb struct {
	Code    int
	Message string
}

func (ew *ErrWeb) UnmarshalJSON(data []byte) error {
	var errWeb struct {
		Code    int
		Message string
	}

	err := json.Unmarshal(data, &errWeb)
	if err != nil {
		return err
	}

	ew.Code = errWeb.Code
	ew.Message = errWeb.Message
	return nil
}

func (ew *ErrWeb) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Code    int
		Message string
	}{
		Code:    ew.Code,
		Message: ew.Message,
	})
}

func (ew *ErrWeb) Error() string {
	return ew.Message
}

type ErrNode = ErrWeb

var RPCErrors = jsonrpc.NewErrors()

func ErrorIsIn(err error, errorTypes []error) bool {
	for _, eType := range errorTypes {
		tmp := reflect.New(reflect.PointerTo(reflect.ValueOf(eType).Elem().Type())).Interface()
		if errors.As(err, tmp) {
			return true
		}
	}
	return false
}

func init() {
	RPCErrors.Register(EUnknown, new(*ErrUnknown))
	RPCErrors.Register(EWeb, new(*ErrWeb))
}
