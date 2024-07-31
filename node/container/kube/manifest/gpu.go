package manifest

type GPU struct {
	Units      ResourceValue
	Attributes Attributes
}

func NewGPU(gpu uint64) *GPU {
	attr := Attribute{Key: "vendor", Value: "nvidia"}
	return &GPU{Units: NewResourceValue(gpu), Attributes: []Attribute{attr}}
}
