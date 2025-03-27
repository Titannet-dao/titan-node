package main

import (
	"fmt"
	"os"

	"github.com/Filecoin-Titan/titan/node/scheduler/projects"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteMapEncodersToFile("../cbor_gen.go", "projects",
		projects.ProjectInfo{},
		projects.ProjectRequirement{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
