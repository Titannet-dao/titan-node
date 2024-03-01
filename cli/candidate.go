package cli

import "github.com/urfave/cli/v2"

var CandidateCmds = []*cli.Command{
	nodeInfoCmd,
	cacheStatCmd,
	progressCmd,
	keyCmds,
	configCmds,
}
