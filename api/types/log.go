package types

type Log string

type ServiceLog struct {
	ServiceName string
	Logs        []Log
}
