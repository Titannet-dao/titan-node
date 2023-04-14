package api

import "context"

// Validation is an interface for validate-related operations
type Validation interface {
	// ExecuteValidation check node asset and bandwidth
	ExecuteValidation(ctx context.Context, req *ValidateReq) error //perm:read
}

// ValidateReq represents the request parameters for validation
type ValidateReq struct {
	// TCPSrvAddr Candidate tcp server address
	TCPSrvAddr string
	RandomSeed int64
	Duration   int
}

// TODO: new tcp package, add these to tcp package
// candidate use tcp server to validate edge
type TCPMsgType int

const (
	TCPMsgTypeNodeID TCPMsgType = iota + 1
	TCPMsgTypeBlock
	TCPMsgTypeCancel
)
