package builder

type SchedulerResourceGPU struct {
	Vendor string `json:"vendor"`
	Model  string `json:"model"`
}

type SchedulerResources struct {
	GPU *SchedulerResourceGPU `json:"gpu"`
}

type SchedulerParams struct {
	RuntimeClass string              `json:"runtime_class"`
	Resources    *SchedulerResources `json:"resources,omitempty"`
	// Affinity     *Affinity `json:"affinity"`
}

type ClusterSettings struct {
	SchedulerParams []*SchedulerParams `json:"scheduler_params"`
}
