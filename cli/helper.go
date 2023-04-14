package cli

import (
	"fmt"
	ufcli "github.com/urfave/cli/v2"
)

type PrintHelpErr struct {
	Err error
	Ctx *ufcli.Context
}

func (e *PrintHelpErr) Error() string {
	return e.Err.Error()
}

func (e *PrintHelpErr) Unwrap() error {
	return e.Err
}

func (e *PrintHelpErr) Is(o error) bool {
	_, ok := o.(*PrintHelpErr)
	return ok
}

func ShowHelp(cctx *ufcli.Context, err error) error {
	return &PrintHelpErr{Err: err, Ctx: cctx}
}

func IncorrectNumArgs(cctx *ufcli.Context) error {
	return ShowHelp(cctx, fmt.Errorf("incorrect number of arguments, got %d", cctx.NArg()))
}
