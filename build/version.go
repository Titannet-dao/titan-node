package build

import "os"

var (
	CurrentCommit string
	BuildType     int
)

const (
	BuildDefault = 0
	BuildMainnet = 0x1
)

func BuildTypeString() string {
	switch BuildType {
	case BuildDefault:
		return ""
	case BuildMainnet:
		return "+mainnet"
	default:
		return "+huh?"
	}
}

// BuildVersion is the local build version
const BuildVersion = "0.1.10"

func UserVersion() string {
	if os.Getenv("TITAN_VERSION_IGNORE_COMMIT") == "1" {
		return BuildVersion
	}

	return BuildVersion + BuildTypeString() + CurrentCommit
}
