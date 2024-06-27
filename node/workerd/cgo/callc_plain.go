//go:build !edge

package cgo

import "errors"

func callC(inputs string) (string, error) {
	return "", errors.New("C function failed")
}
