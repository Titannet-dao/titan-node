package projects

import (
	"golang.org/x/xerrors"
)

type mutator interface {
	apply(state *ProjectInfo)
}

// globalMutator is an event which can apply in every state
type globalMutator interface {
	// applyGlobal applies the event to the state. If if returns true,
	//  event processing should be interrupted
	applyGlobal(state *ProjectInfo) bool
}

// Ignorable Ignorable
type Ignorable interface {
	Ignore()
}

// Global events

// ProjectRestart restarts project
type ProjectRestart struct{}

func (evt ProjectRestart) applyGlobal(state *ProjectInfo) bool {
	state.RetryCount = 0
	return false
}

// ProjectForceState forces an project state
type ProjectForceState struct {
	State   ProjectState
	Details string
	NodeIDs []string
	Event   int64
}

func (evt ProjectForceState) applyGlobal(state *ProjectInfo) bool {
	state.State = evt.State
	state.RetryCount = 0
	// state.Requirement.NodeIDs = evt.NodeIDs
	state.Event = evt.Event
	return true
}

// DeployResult represents the result of node starting
type DeployResult struct {
	BlocksCount int64
	Size        int64
}

func (evt DeployResult) apply(state *ProjectInfo) {
}

// Ignore is a method that indicates whether the deployment result should be ignored.
func (evt DeployResult) Ignore() {
}

// DeployRequestSent indicates that a pull request has been sent
type DeployRequestSent struct{}

func (evt DeployRequestSent) apply(state *ProjectInfo) {
}

// UpdateRequestSent indicates that a pull request has been sent
type UpdateRequestSent struct{}

func (evt UpdateRequestSent) apply(state *ProjectInfo) {
}

// ProjectRedeploy re-pull the project
type ProjectRedeploy struct{}

func (evt ProjectRedeploy) apply(state *ProjectInfo) {
	state.RetryCount++
}

// Ignore is a method that allows ignoring the redeploy event.
func (evt ProjectRedeploy) Ignore() {
}

// DeploySucceed indicates that a node has successfully pulled an project
type DeploySucceed struct{}

func (evt DeploySucceed) apply(state *ProjectInfo) {
	state.RetryCount = 0
}

// Ignore is a method that indicates the deployment succeeded and should be ignored.
func (evt DeploySucceed) Ignore() {
}

// SkipStep skips the current step
type SkipStep struct{}

func (evt SkipStep) apply(state *ProjectInfo) {}

// DeployFailed indicates that a node has failed to pull an project
type DeployFailed struct{ error }

// FormatError Format error
func (evt DeployFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt DeployFailed) apply(state *ProjectInfo) {
	state.RetryCount = 1
}

// Ignore handles the deployment failure by ignoring the event.
func (evt DeployFailed) Ignore() {
}

// CreateFailed  indicates that node selection has failed
type CreateFailed struct{ error }

// FormatError Format error
func (evt CreateFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt CreateFailed) apply(state *ProjectInfo) {
}

// UpdateFailed  indicates that node selection has failed
type UpdateFailed struct{ error }

// FormatError Format error
func (evt UpdateFailed) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt UpdateFailed) apply(state *ProjectInfo) {
}
