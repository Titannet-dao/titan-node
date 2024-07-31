package manifest

type ServiceProtocol string

const (
	TCP = ServiceProtocol("TCP")
	UDP = ServiceProtocol("UDP")
)

func (sp ServiceProtocol) ToString() string {
	return string(sp)
}

// ServiceExpose stores exposed ports and hosts details
type ServiceExpose struct {
	// request
	Port         uint32
	ExternalPort uint32
	// request
	Proto                  ServiceProtocol
	Service                string
	Global                 bool
	Hosts                  []string
	HTTPOptions            ServiceExposeHTTPOptions
	IP                     string
	EndpointSequenceNumber uint32
}
