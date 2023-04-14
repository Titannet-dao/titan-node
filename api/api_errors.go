package api

import (
	"errors"
	"reflect"

	"github.com/filecoin-project/go-jsonrpc"
)

const (
	EUnknown = iota + jsonrpc.FirstUserCode
)

type ErrUnknown struct{}

func (e *ErrUnknown) Error() string {
	return "unknown"
}

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
}
