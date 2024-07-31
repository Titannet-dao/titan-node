package manifest

type ResourceUnits struct {
	CPU       *CPU
	Memory    *Memory
	Storage   []*Storage
	GPU       *GPU
	Endpoints []*Endpoint
}

func NewResourceUnits(cpu, gpu, memory uint64, storage []*Storage) *ResourceUnits {
	return &ResourceUnits{CPU: NewCPU(cpu), Memory: NewMemory(memory), Storage: storage, GPU: NewGPU(gpu)}
}
