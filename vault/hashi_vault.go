// Package vault implements the Hashi Vault integration
package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/accuknox/kmux/config"
	"github.com/golang-jwt/jwt"
	vault "github.com/hashicorp/vault/api"
	auth "github.com/hashicorp/vault/api/auth/kubernetes"
	"github.com/rs/zerolog/log"
)

// HashiVault contains Role, SecretPath and BasePath to connect with the vault
type HashiVault struct {
	Role       string
	SecretPath string
	BasePath   string
}

type token struct {
	KubernetesIO k8s `json:"kubernetes.io"`
}

type k8s struct {
	Namespace      string         `json:"namespace"`
	ServiceAccount serviceAccount `json:"serviceaccount"`
}
type serviceAccount struct {
	Name string `json:"name"`
}

// GetSecret - authenticates the client and get the secrets
func (v *HashiVault) GetSecret(k string) (interface{}, error) {

	conf := config.Vault
	config := vault.DefaultConfig()

	config.Address = conf.Server

	client, err := vault.NewClient(config)
	if err != nil {
		log.Error().Msgf("unable to initialize Vault client. %v", err)
		return nil, err
	}

	if client.Token() == "" {
		log.Info().Msgf("Authenticating into Vault [%s] with role [%s]", conf.Server, v.Role)
		err = authWithServiceAccountToken(client, v.Role)
		if err != nil {
			log.Error().Msgf("Failed to authenticate in Hashi Vault. %v", err)
			return nil, err
		}
	}

	log.Info().Msgf("Accessing secrets at [%s/%s]", v.BasePath, v.SecretPath)
	secret, err := client.KVv1(v.BasePath).Get(context.Background(), v.SecretPath)
	if err != nil {
		log.Error().Msgf("Failed to read secret: %v", err)
		return nil, err
	}

	value := secret.Data[k]
	if value == nil {
		return nil, fmt.Errorf("nil value for '%v' field", k)
	}
	return value, err
}

func authWithServiceAccountToken(client *vault.Client, role string) error {
	k8sAuth, err := auth.NewKubernetesAuth(
		role,
	)
	if err != nil {
		return fmt.Errorf("unable to initialize Kubernetes auth method: %w", err)
	}

	authInfo, err := client.Auth().Login(context.TODO(), k8sAuth)
	if err != nil {
		return fmt.Errorf("unable to log in with Kubernetes auth: %w", err)
	}
	if authInfo == nil {
		return fmt.Errorf("no auth info was returned after login")
	}
	return err
}

// NewHashiVault - returns HashiVault with data extracted from "/var/run/secrets/kubernetes.io/serviceaccount/token"
func NewHashiVault(secretPath string) *HashiVault {

	var tokeninfo token
	HashiVault := HashiVault{}

	secret := strings.FieldsFunc(secretPath, Split)
	HashiVault.BasePath = secret[0]
	HashiVault.SecretPath = secret[1] + "/" + secret[2]

	serviceaccountToken, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		log.Error().Msgf("Failed to read service account token. %v", err)
		return nil
	}

	token := strings.FieldsFunc(string(serviceaccountToken), Split)
	tokenDecoded, _ := jwt.DecodeSegment(token[1])

	err = json.Unmarshal(tokenDecoded, &tokeninfo)
	if err != nil {
		log.Error().Msgf("Failed to decode service account token. : %v", err)
		return nil
	}

	HashiVault.Role = tokeninfo.KubernetesIO.Namespace + "_" + tokeninfo.KubernetesIO.ServiceAccount.Name
	return &HashiVault
}

// Split string at charcters '/' and '.'
func Split(r rune) bool {
	return r == '/' || r == '.'
}
