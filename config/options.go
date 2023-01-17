package config

const (
	defaultConfigFile = "kmux-config.yaml"
)

// Options contains kmux initialization options
type Options struct {
	LocalConfigFile string
}

func (o *Options) getLocalConfigFile() string {
	if o == nil || o.LocalConfigFile == "" {
		return defaultConfigFile
	}
	return o.LocalConfigFile
}
