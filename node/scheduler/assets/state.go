package assets

// AssetState represents the state of an asset in the process of being pulled.
type AssetState string

// Constants defining various states of the asset pulling process.
const (
	// SeedSelect select first candidate to pull seed asset
	SeedSelect AssetState = "SeedSelect"
	// SeedPulling Waiting for candidate nodes to pull seed asset
	SeedPulling AssetState = "SeedPulling"
	// SeedFailed Unable to select candidate nodes or failed to pull seed asset
	SeedFailed AssetState = "SeedFailed"

	// SeedSync sync seed from other scheduler
	SeedSync AssetState = "SeedSync"
	// SeedSyncing sync seed from other scheduler
	SeedSyncing AssetState = "SeedSyncing"
	// SyncFailed Unable to select candidate nodes or failed to pull seed asset
	SyncFailed AssetState = "SyncFailed"

	// UploadInit Initialize user upload preparation
	UploadInit AssetState = "UploadInit"
	// SeedUploading Waiting for user to upload asset to candidate node
	SeedUploading AssetState = "SeedUploading"
	// UploadFailed User failed to upload assets
	UploadFailed AssetState = "UploadFailed"

	// CandidatesSelect select candidates to pull asset
	CandidatesSelect AssetState = "CandidatesSelect"
	// CandidatesPulling candidate nodes pulling asset
	CandidatesPulling AssetState = "CandidatesPulling"
	// CandidatesFailed Unable to select candidate nodes or failed to pull asset
	CandidatesFailed AssetState = "CandidatesFailed"

	// EdgesSelect select edges to pull asset
	EdgesSelect AssetState = "EdgesSelect"
	// EdgesPulling edge nodes pulling asset
	EdgesPulling AssetState = "EdgesPulling"
	// EdgesFailed  Unable to select edge nodes or failed to pull asset
	EdgesFailed AssetState = "EdgesFailed"

	// Servicing Asset cache completed and in service
	Servicing AssetState = "Servicing"
	// Remove remove
	Remove AssetState = "Remove"
	// Stop Stop
	Stop AssetState = "Stop"
)

// String returns the string representation of the AssetState.
func (s AssetState) String() string {
	return string(s)
}

var (
	// FailedStates contains a list of asset pull states that represent failures.
	FailedStates = []string{
		SeedFailed.String(),
		CandidatesFailed.String(),
		EdgesFailed.String(),
		UploadFailed.String(),
		SyncFailed.String(),
	}

	// PullingStates contains a list of asset pull states that represent pulling.
	PullingStates = []string{
		SeedSelect.String(),
		SeedSync.String(),
		SeedPulling.String(),
		UploadInit.String(),
		SeedUploading.String(),
		SeedSyncing.String(),
		CandidatesSelect.String(),
		CandidatesPulling.String(),
		EdgesSelect.String(),
		EdgesPulling.String(),
	}

	// ActiveStates contains a list of asset pull states that represent active.
	ActiveStates = append(append([]string{Servicing.String(), Stop.String()}, FailedStates...), PullingStates...)
)
