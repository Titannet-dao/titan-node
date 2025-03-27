package types

// SchedulerCfg scheduler config
type SchedulerCfg struct {
	SchedulerURL string `db:"scheduler_url"`
	AreaID       string `db:"area_id"`
	Weight       int    `db:"weight"`
	AccessToken  string `db:"access_token"`
}

// MinioConfig holds the configuration for connecting to MinIO.
type MinioConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
}
