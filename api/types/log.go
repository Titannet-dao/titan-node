package types

// Log represents a log entry as a string.
type Log string

// ServiceLog represents the log entries for a specific service.
type ServiceLog struct {
	ServiceName string
	Logs        []Log
}
