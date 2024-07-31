package manifest

type Memory struct {
	Quantity   ResourceValue
	Attributes []Attribute
}

func NewMemory(memory uint64) *Memory {
	return &Memory{Quantity: NewResourceValue(memory)}
}
