//go:build darwin
// +build darwin

package build

var (
	DefaultFDLimit uint64 = 16 << 10
	EdgeFDLimit    uint64 = 60_000
)
