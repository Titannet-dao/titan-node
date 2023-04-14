package api

import (
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"

	xerrors "golang.org/x/xerrors"
)

type Version uint32

func newVer(major, minor, patch uint8) Version {
	return Version(uint32(major)<<16 | uint32(minor)<<8 | uint32(patch))
}

// Ints returns (major, minor, patch) versions
func (ve Version) Ints() (uint32, uint32, uint32) {
	v := uint32(ve)
	return (v & majorOnlyMask) >> 16, (v & minorOnlyMask) >> 8, v & patchOnlyMask
}

func (ve Version) String() string {
	vmj, vmi, vp := ve.Ints()
	return fmt.Sprintf("%d.%d.%d", vmj, vmi, vp)
}

func (ve Version) EqMajorMinor(v2 Version) bool {
	return ve&minorMask == v2&minorMask
}

// semver versions of the rpc api exposed
var (
	SchedulerAPIVersion0 = newVer(1, 0, 0)

	CandidateAPIVersion0 = newVer(1, 0, 0)
	EdgeAPIVersion0      = newVer(1, 0, 0)
	LocationAPIVersion0  = newVer(1, 0, 0)
)

//nolint:varcheck,deadcode
const (
	majorMask = 0xff0000
	minorMask = 0xffff00
	patchMask = 0xffffff

	majorOnlyMask = 0xff0000
	minorOnlyMask = 0x00ff00
	patchOnlyMask = 0x0000ff
)

func VersionForType(nodeType types.NodeType) (Version, error) {
	switch nodeType {
	case types.NodeScheduler:
		return SchedulerAPIVersion0, nil
	case types.NodeCandidate:
		return CandidateAPIVersion0, nil
	case types.NodeEdge:
		return EdgeAPIVersion0, nil
	case types.NodeLocator:
		return LocationAPIVersion0, nil
	default:
		return Version(0), xerrors.Errorf("unknown node type %d", nodeType)
	}
}

// EdgeUpdateInfo just update edge node
// NodeType include edge-updater and titan-edge
type EdgeUpdateConfig struct {
	NodeType    int       `db:"node_type"`
	AppName     string    `db:"app_name"`
	Version     Version   `db:"version"`
	DownloadURL string    `db:"download_url"`
	Hash        string    `db:"hash"`
	UpdateTime  time.Time `db:"update_time"`
}
