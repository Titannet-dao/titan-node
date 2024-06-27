package projects

import (
	"reflect"

	"github.com/filecoin-project/go-statemachine"
	"golang.org/x/xerrors"
)

// Plan prepares a plan for project start
func (m *Manager) Plan(events []statemachine.Event, user interface{}) (interface{}, uint64, error) {
	log.Debugf("user:%v , events:%v", user, events)
	next, processed, err := m.plan(events, user.(*ProjectInfo))
	if err != nil || next == nil {
		return nil, processed, nil
	}

	return func(ctx statemachine.Context, si ProjectInfo) error {
		err := next(ctx, si)
		if err != nil {
			log.Errorf("unhandled error (%s): %+v", si.UUID, err)
			return nil
		}

		return nil
	}, processed, nil
}

// maps project states to their corresponding planner functions
var planners = map[ProjectState]func(events []statemachine.Event, state *ProjectInfo) (uint64, error){
	// external import
	NodeSelect: planOne(
		on(DeployRequestSent{}, Deploying),
		on(CreateFailed{}, Failed),
		on(SkipStep{}, Servicing),
	),
	Update: planOne(
		on(UpdateRequestSent{}, Deploying),
		on(UpdateFailed{}, Failed),
		on(SkipStep{}, Servicing),
	),
	Deploying: planOne(
		on(DeploySucceed{}, Servicing),
		on(DeployFailed{}, Failed),
		apply(DeployResult{}),
	),
	Failed: planOne(
		on(ProjectRedeploy{}, NodeSelect),
	),
	Remove:    planOne(),
	Servicing: planOne(),
}

// plan creates a plan for the next project  action based on the given events and project state
func (m *Manager) plan(events []statemachine.Event, state *ProjectInfo) (func(statemachine.Context, ProjectInfo) error, uint64, error) {
	log.Debugf("hash:%s , state:%s , events:%v", state.UUID, state.State, events)
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

	log.Debugf("%s: %s", state.UUID, state.State)

	switch state.State {
	// Happy path
	case NodeSelect:
		return m.handleNodeSelect, processed, nil
	case Update:
		return m.handleUpdate, processed, nil
	case Deploying:
		return m.handleDeploying, processed, nil
	case Servicing:
		return m.handleServicing, processed, nil
	case Failed:
		return m.handleDeploysFailed, processed, nil
	case Remove:
		return m.handleRemove, processed, nil
	// Fatal errors
	default:
		log.Errorf("unexpected project update state: %s", state.State)
	}

	return nil, processed, nil
}

// prepares a single plan for a given project state, allowing for one event at a time
func planOne(ts ...func() (mut mutator, next func(info *ProjectInfo) (more bool, err error))) func(events []statemachine.Event, state *ProjectInfo) (uint64, error) {
	return func(events []statemachine.Event, state *ProjectInfo) (uint64, error) {
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
					log.Warnf("project %s:%s got error event %T: %+v", state.UUID, state.UUID, event.User, err)
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
func on(mut mutator, next ProjectState) func() (mutator, func(*ProjectInfo) (bool, error)) {
	return func() (mutator, func(*ProjectInfo) (bool, error)) {
		return mut, func(state *ProjectInfo) (bool, error) {
			state.State = next
			return false, nil
		}
	}
}

// apply like `on`, but doesn't change state
func apply(mut mutator) func() (mutator, func(*ProjectInfo) (bool, error)) {
	return func() (mutator, func(*ProjectInfo) (bool, error)) {
		return mut, func(state *ProjectInfo) (bool, error) {
			return true, nil
		}
	}
}

// initStateMachines init all project state machines
func (m *Manager) initStateMachines() error {
	// initialization
	defer m.stateMachineWait.Done()

	list, err := m.ListProjects()
	if err != nil {
		return err
	}

	pullingHashs := make([]string, 0)

	for _, project := range list {
		if project.State == Remove || project.State == Servicing {
			continue
		}

		if err := m.projectStateMachines.Send(project.UUID, ProjectRestart{}); err != nil {
			log.Errorf("initStateMachines project send %s , err %s", project.UUID, err.Error())
			continue
		}

		pullingHashs = append(pullingHashs, project.UUID.String())

		m.startProjectTimeoutCounting(project.UUID.String(), 0)
	}
	//
	// m.DeleteReplicaOfTimeout(PullingStates, pullingHashs)

	return nil
}

// ListProjects load project pull infos from state machine
func (m *Manager) ListProjects() ([]ProjectInfo, error) {
	var list []ProjectInfo
	if err := m.projectStateMachines.List(&list); err != nil {
		return nil, err
	}
	return list, nil
}
