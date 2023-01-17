// Package config implements the routines required for kmux configuration management
package config

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	defaultK8sConfigMap = "kmux"
	k8sConfigMapKey     = "config.yaml"
)

const (
	// PulsarDriver specifies the source/sink instance uses Apache Pulsar
	PulsarDriver = "pulsar"

	// KnoxGatewayDriver specifies the sink instance uses Accuknox gRPC gateway
	KnoxGatewayDriver = "knox-gateway"

	// MySQLDriver specifies that the source/sink instance uses MySQL database
	MySQLDriver = "mysql"

	// PostgreSQLDriver specifies that the source/sink instance uses PostgreSQL database
	PostgreSQLDriver = "postgres"

	// SQLiteDriver specifies that the source/sink instance uses SQLite v3 database
	SQLiteDriver = "sqlite"

	// MongoDBDriver specifies that the source/sink instance uses MongoDB database
	MongoDBDriver = "mongodb"

	// HashiVaultDriver specifies that the Vault instance uses HashiVault
	HashiVaultDriver = "hashiVault"
)

// InterfaceConfig contains the name of stream (or) database
// driver used in the application
type InterfaceConfig struct {
	StreamDriver   string
	DatabaseDriver string
}

// AppConfig contains source and sink configuration
type AppConfig struct {
	Sink   InterfaceConfig
	Source InterfaceConfig
	Vault  string
}

// PulsarConfig contains Apache Pulsar related configuration
type PulsarConfig struct {
	TopicPrefix  string
	Options      pulsar.ClientOptions
	Subscription string
}

// DatabaseVaultKey contains key for vault secrets
type DatabaseVaultKey struct {
	Username string
	Password string
}

// DatabaseVault contains vault object from config file
type DatabaseVault struct {
	SecretPath string
	Key        DatabaseVaultKey
}

// DatabaseConfig is a generic type for storing database related configuration
type DatabaseConfig struct {
	Server     string
	Name       string
	Username   string
	Password   string
	Vault      *DatabaseVault
	ConnParams []string
}

// HashiVaultConfig contains Server address to connect with Vault
type HashiVaultConfig struct {
	Server string
}

// KnoxGatewayConfig contains AccuKnox GRPC Gateway related configuration
type KnoxGatewayConfig struct {
	Server string
}

// App holds the information about source and sink drivers used in the application
var App AppConfig

// Pulsar configurations
var Pulsar PulsarConfig

// KnoxGateway configurations
var KnoxGateway KnoxGatewayConfig

// Database holds generic database configurations
var Database DatabaseConfig

// Vault - HashiVaultConfig configurations
var Vault HashiVaultConfig

// Viper instance to parse kmux configuration
var Viper *viper.Viper

// Init initializes kmux configuration
func Init(options *Options) error {
	Viper = viper.New()

	err := loadConfigFromK8s()
	if err != nil {
		log.Error().Msgf("Failed to load kmux configuration from k8s config-map. %s", err)

		// Try loading the config from local config file
		configFile := options.getLocalConfigFile()
		log.Info().Msgf("Using local kmux config file %s", configFile)
		if err = loadConfigFromFile(configFile); err != nil {
			log.Error().Msgf("Failed to load local kmux configuration. %s", err)
			return err
		}
	} else {
		log.Info().Msg("Loaded kmux configuration from k8s config-map")
	}

	printCurrentConfig()

	populateAppConfig()
	populatePulsarConfig()
	populateKnoxGatewayConfig()
	populateDatabaseConfig()
	populateVaultConfig()

	return nil
}

func getK8sPodNamespace() (string, error) {
	data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func getConfigFromK8sConfigMap(namespace string) ([]byte, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(context.TODO(), defaultK8sConfigMap, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	configYaml := cm.Data[k8sConfigMapKey]
	return []byte(configYaml), nil
}

func loadConfigFromK8s() error {
	ns, err := getK8sPodNamespace()
	if err != nil {
		return err
	}

	configYaml, err := getConfigFromK8sConfigMap(ns)
	if err != nil {
		return err
	}

	Viper.SetConfigType("yaml")
	err = Viper.ReadConfig(bytes.NewBuffer(configYaml))
	if err != nil {
		return err
	}
	return nil
}

func loadConfigFromFile(file string) error {
	Viper.SetConfigFile(file)
	err := Viper.ReadInConfig()
	if err != nil {
		return err
	}
	return nil
}

func populateAppConfig() {
	App.Sink.StreamDriver = Viper.GetString("kmux.sink.stream")
	App.Sink.DatabaseDriver = Viper.GetString("kmux.sink.database")
	App.Source.StreamDriver = Viper.GetString("kmux.source.stream")
	App.Source.DatabaseDriver = Viper.GetString("kmux.source.database")
	App.Vault = Viper.GetString("kmux.vault")
}

func populatePulsarConfig() {
	servers := Viper.GetStringSlice("pulsar.servers")
	subscription := Viper.GetString("pulsar.subscription")
	encryptEnabled := Viper.GetBool("pulsar.encryption.enable")
	authEnabled := Viper.GetBool("pulsar.auth.enable")

	opt := pulsar.ClientOptions{}
	if encryptEnabled {
		opt.URL = fmt.Sprintf("pulsar+ssl://%s", strings.Join(servers, ","))
		opt.TLSTrustCertsFilePath = Viper.GetString("pulsar.encryption.ca-cert")
		if authEnabled {
			keyPath := Viper.GetString("pulsar.auth.key")
			certPath := Viper.GetString("pulsar.auth.cert")
			opt.Authentication = pulsar.NewAuthenticationTLS(certPath, keyPath)
		}
	} else {
		opt.URL = fmt.Sprintf("pulsar://%s", strings.Join(servers, ","))
	}

	prefix := Viper.GetString("pulsar.topic-prefix")

	Pulsar = PulsarConfig{
		TopicPrefix:  prefix,
		Options:      opt,
		Subscription: subscription,
	}
}

func populateDatabaseConfig() {
	Database = DatabaseConfig{
		Server:     Viper.GetString("database.server"),
		Name:       Viper.GetString("database.name"),
		ConnParams: Viper.GetStringSlice("database.connectionParams"),
	}
	if Viper.Get("database.vault") == nil {
		Database.Username = Viper.GetString("database.username")
		Database.Password = Viper.GetString("database.password")
		Database.Vault = nil
	} else {
		Database.Username = ""
		Database.Password = ""
		Database.Vault = &DatabaseVault{
			SecretPath: Viper.GetString("database.vault.secretPath"),
			Key: DatabaseVaultKey{
				Username: Viper.GetString("database.vault.key.username"),
				Password: Viper.GetString("database.vault.key.password"),
			},
		}
	}
}

func populateVaultConfig() {
	Vault = HashiVaultConfig{
		Server: Viper.GetString("vault.server"),
	}
}

func populateKnoxGatewayConfig() {
	KnoxGateway = KnoxGatewayConfig{
		Server: Viper.GetString("knox-gateway.server"),
	}
}
func printCurrentConfig() {
	allKeys := Viper.AllKeys()

	configArr := []string{}
	for _, key := range allKeys {
		value := Viper.Get(key)
		configArr = append(configArr, fmt.Sprintf("%s=%v", key, value))
	}

	log.Info().Msgf("Kmux current configuration - [%s]", strings.Join(configArr, ", "))
}
