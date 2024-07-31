package manifest

type CPU struct {
	Units      ResourceValue
	Attributes []Attribute
}

func NewCPU(cpu uint64) *CPU {
	return &CPU{Units: NewResourceValue(cpu)}
}
