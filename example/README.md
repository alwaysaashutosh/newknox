## Configuration Management
kmux gets the configuration from either one of the following sources.
1. K8s config-map
2. Local configuration file

#### K8s ConfigMap
During `Init()`, kmux looks up for a k8s config-map named `kmux` in the same namespace in which the microservice is running. If the config-map exists, then kmux uses it for initialization. Otherwise, kmux fallbacks to using local configuration file. For example, refer [the sample config-map file](kmux-k8s-configmap.yaml).

#### Local Configuration File
Whenever k8s config-map lookup fails, kmux fallbacks to using local configuration file. kmux looks up for a file named `kmux-config.yaml` (by default) in the current working directory. If the file exists, then kmux uses it for initialization. CLI argument `--kmux-config <file-path>` can be used to override default file path.