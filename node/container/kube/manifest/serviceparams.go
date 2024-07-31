package manifest

type ServiceParams struct {
	Storage []StorageParams
}

type StorageParams struct {
	Name     string
	Mount    string
	ReadOnly bool
}
