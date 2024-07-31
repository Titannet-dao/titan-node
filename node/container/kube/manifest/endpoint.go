package manifest

type Kind int

const (
	ShareHTTP Kind = iota
	RandomPort
	LEASED_IP
)

type Endpoint struct {
	kind           Kind
	sequenceNumber uint32
}
