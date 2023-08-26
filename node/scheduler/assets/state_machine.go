package assets

import (
	"context"
	"reflect"

	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

// Plan prepares a plan for asset pulling
func (m *Manager) Plan(events []statemachine.Event, user interface{}) (interface{}, uint64, error) {
	next, processed, err := m.plan(events, user.(*AssetPullingInfo))
	if err != nil || next == nil {
		return nil, processed, nil
	}

	return func(ctx statemachine.Context, si AssetPullingInfo) error {
		err := next(ctx, si)
		if err != nil {
			log.Errorf("unhandled error (%s): %+v", si.CID, err)
			return nil
		}

		return nil
	}, processed, nil
}

// maps asset states to their corresponding planner functions
var planners = map[AssetState]func(events []statemachine.Event, state *AssetPullingInfo) (uint64, error){
	// external import
	SeedSelect: planOne(
		on(PullRequestSent{}, SeedPulling),
		on(SelectFailed{}, SeedFailed),
		on(SkipStep{}, CandidatesSelect),
	),
	SeedPulling: planOne(
		on(PullSucceed{}, CandidatesSelect),
		on(PullFailed{}, SeedFailed),
		apply(PulledResult{}),
	),
	UploadInit: planOne(
		on(PullRequestSent{}, SeedUploading),
		on(SelectFailed{}, UploadFailed),
		on(SkipStep{}, CandidatesSelect),
	),
	SeedUploading: planOne(
		on(PullSucceed{}, CandidatesSelect),
		on(PullFailed{}, UploadFailed),
		apply(PulledResult{}),
	),
	CandidatesSelect: planOne(
		on(PullRequestSent{}, CandidatesPulling),
		on(SkipStep{}, EdgesSelect),
		on(SelectFailed{}, CandidatesFailed),
	),
	CandidatesPulling: planOne(
		on(PullFailed{}, CandidatesFailed),
		on(PullSucceed{}, EdgesSelect),
		apply(PulledResult{}),
	),
	EdgesSelect: planOne(
		on(PullRequestSent{}, EdgesPulling),
		on(SelectFailed{}, EdgesFailed),
		on(SkipStep{}, Servicing),
	),
	EdgesPulling: planOne(
		on(PullFailed{}, EdgesFailed),
		on(PullSucceed{}, Servicing),
		apply(PulledResult{}),
	),
	SeedFailed: planOne(
		on(AssetRePull{}, SeedSelect),
	),
	CandidatesFailed: planOne(
		on(AssetRePull{}, CandidatesSelect),
	),
	EdgesFailed: planOne(
		on(AssetRePull{}, EdgesSelect),
	),
	UploadFailed: planOne(),
	Remove:       planOne(),
	Servicing:    planOne(),
}

// plan creates a plan for the next asset pulling action based on the given events and asset state
func (m *Manager) plan(events []statemachine.Event, state *AssetPullingInfo) (func(statemachine.Context, AssetPullingInfo) error, uint64, error) {
	log.Debugf("state:%s , events:%v", state.State, events)
	p := planners[state.State]
	if p == nil {
		if len(events) == 1 {
			if _, ok := events[0].User.(globalMutator); ok {
				p = planOne() // in case we're in a really weird state, allow restart / update state / remove
			}
		}

		if p == nil {
			return nil, 0, xerrors.Errorf("planner for state %s not found", state.State)
		}
	}

	processed, err := p(events, state)
	if err != nil {
		return nil, processed, xerrors.Errorf("running planner for state %s failed: %w", state.State, err)
	}

	log.Debugf("%s: %s", state.Hash, state.State)

	switch state.State {
	// Happy path
	case SeedSelect:
		return m.handleSeedSelect, processed, nil
	case SeedPulling:
		return m.handleSeedPulling, processed, nil
	case UploadInit:
		return m.handleUploadInit, processed, nil
	case SeedUploading:
		return m.handleSeedUploading, processed, nil
	case CandidatesSelect:
		return m.handleCandidatesSelect, processed, nil
	case EdgesSelect:
		return m.handleEdgesSelect, processed, nil
	case CandidatesPulling:
		return m.handleCandidatesPulling, processed, nil
	case EdgesPulling:
		return m.handleEdgesPulling, processed, nil
	case Servicing:
		return m.handleServicing, processed, nil
	case SeedFailed, CandidatesFailed, EdgesFailed:
		return m.handlePullsFailed, processed, nil
	case UploadFailed:
		return m.handleUploadFailed, processed, nil
	case Remove:
		return m.handleRemove, processed, nil
	// Fatal errors
	default:
		log.Errorf("unexpected asset update state: %s", state.State)
	}

	return nil, processed, nil
}

// prepares a single plan for a given asset state, allowing for one event at a time
func planOne(ts ...func() (mut mutator, next func(info *AssetPullingInfo) (more bool, err error))) func(events []statemachine.Event, state *AssetPullingInfo) (uint64, error) {
	return func(events []statemachine.Event, state *AssetPullingInfo) (uint64, error) {
	eloop:
		for i, event := range events {
			if gm, ok := event.User.(globalMutator); ok {
				gm.applyGlobal(state)
				return uint64(i + 1), nil
			}

			for _, t := range ts {
				mut, next := t()

				if reflect.TypeOf(event.User) != reflect.TypeOf(mut) {
					continue
				}

				if err, isErr := event.User.(error); isErr {
					log.Warnf("asset %s got error event %T: %+v", state.CID, event.User, err)
				}

				event.User.(mutator).apply(state)
				more, err := next(state)
				if err != nil || !more {
					return uint64(i + 1), err
				}

				continue eloop
			}

			_, ok := event.User.(Ignorable)
			if ok {
				continue
			}

			return uint64(i + 1), xerrors.Errorf("planner for state %s received unexpected event %T (%+v)", state.State, event.User, event)
		}

		return uint64(len(events)), nil
	}
}

// on is a utility function to handle state transitions
func on(mut mutator, next AssetState) func() (mutator, func(*AssetPullingInfo) (bool, error)) {
	return func() (mutator, func(*AssetPullingInfo) (bool, error)) {
		return mut, func(state *AssetPullingInfo) (bool, error) {
			state.State = next
			return false, nil
		}
	}
}

// apply like `on`, but doesn't change state
func apply(mut mutator) func() (mutator, func(*AssetPullingInfo) (bool, error)) {
	return func() (mutator, func(*AssetPullingInfo) (bool, error)) {
		return mut, func(state *AssetPullingInfo) (bool, error) {
			return true, nil
		}
	}
}

// initStateMachines init all asset state machines
func (m *Manager) initStateMachines(ctx context.Context) error {
	// initialization
	defer m.stateMachineWait.Done()

	list, err := m.ListAssets()
	if err != nil {
		return err
	}

	for _, asset := range list {
		if asset.State == Remove || asset.State == Servicing {
			continue
		}

		if err := m.assetStateMachines.Send(asset.Hash, PullAssetRestart{}); err != nil {
			log.Errorf("initStateMachines asset send %s , err %s", asset.CID, err.Error())
			continue
		}

		m.startAssetTimeoutCounting(asset.Hash.String(), 0)
	}

	return nil
}

// ListAssets load asset pull infos from state machine
func (m *Manager) ListAssets() ([]AssetPullingInfo, error) {
	var list []AssetPullingInfo
	if err := m.assetStateMachines.List(&list); err != nil {
		return nil, err
	}
	return list, nil
}
