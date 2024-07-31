package api

import "context"

// Candidate is an interface for candidate node
type L5 interface {
	Common
	WaitQuiet(ctx context.Context) error //perm:admin
}
