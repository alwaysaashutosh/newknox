package vault

import (
	"fmt"

	"github.com/accuknox/kmux/config"
)

// Vault defines GetSecret() method
type Vault interface {
	GetSecret(k string) (interface{}, error)
}

// NewVault - checks the drivers type and return NewHashiVault
func NewVault(secretPath string) (Vault, error) {
	driver := config.App.Vault
	if driver == config.HashiVaultDriver {
		return NewHashiVault(secretPath), nil
	}
	return nil, fmt.Errorf("vault driver %s not supported", config.App.Vault)
}
