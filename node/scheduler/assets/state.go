package assets

// AssetState represents the state of an asset in the process of being pulled.
type AssetState string

// Constants defining various states of the asset pulling process.
const (
	// UndefinedState represents an undefined state.
	UndefinedState AssetState = ""
	// SeedSelect select first candidate to pull seed asset
	SeedSelect AssetState = "SeedSelect"
	// SeedPulling Waiting for candidate nodes to pull seed asset
	SeedPulling AssetState = "SeedPulling "
	// CandidatesSelect select candidates to pull asset
	CandidatesSelect AssetState = "CandidatesSelect"
	// CandidatesPulling candidate nodes pulling asset
	CandidatesPulling AssetState = "CandidatesPulling"
	// EdgesSelect select edges to pull asset
	EdgesSelect AssetState = "EdgesSelect"
	// EdgesPulling edge nodes pulling asset
	EdgesPulling AssetState = "EdgesPulling"
	// Servicing Asset cache completed and in service
	Servicing AssetState = "Servicing"
	// SeedFailed Unable to select candidate nodes or failed to pull seed asset
	SeedFailed AssetState = "SeedFailed"
	// CandidatesFailed Unable to select candidate nodes or failed to pull asset
	CandidatesFailed AssetState = "CandidatesFailed"
	// EdgesFailed  Unable to select edge nodes or failed to pull asset
	EdgesFailed AssetState = "EdgesFailed"
	// Remove remove
	Remove AssetState = "Remove"
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
	}

	// PullingStates contains a list of asset pull states that represent pulling.
	PullingStates = []string{
		SeedSelect.String(),
		SeedPulling.String(),
		CandidatesSelect.String(),
		CandidatesPulling.String(),
		EdgesSelect.String(),
		EdgesPulling.String(),
	}

	// AllStates contains a list of asset pull states that represent all.
	AllStates = []string{
		SeedSelect.String(),
		SeedPulling.String(),
		CandidatesSelect.String(),
		CandidatesPulling.String(),
		EdgesSelect.String(),
		EdgesPulling.String(),
		SeedFailed.String(),
		CandidatesFailed.String(),
		EdgesFailed.String(),
		Servicing.String(),
	}
)
