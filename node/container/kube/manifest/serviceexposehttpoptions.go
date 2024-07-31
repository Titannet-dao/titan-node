package manifest

type ServiceExposeHTTPOptions struct {
	MaxBodySize uint32
	ReadTimeout uint32
	SendTimeout uint32
	NextTries   uint32
	NextTimeout uint32
	NextCases   []string
}
