package types

type Event string

type ServiceEvent struct {
	ServiceName string
	Events      []Event
}
