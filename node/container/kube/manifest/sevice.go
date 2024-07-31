package manifest

type Service struct {
	Name      string
	Image     string
	Command   []string
	Args      []string
	Env       []string
	Resources *ResourceUnits
	Count     int32
	Expose    []*ServiceExpose
	Params    *ServiceParams
	OSType    string
}
