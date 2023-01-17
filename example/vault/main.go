// Package main
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/accuknox/kmux"
	"github.com/accuknox/kmux/config"
)

func main() {
	// Init
	err := kmux.Init(&config.Options{
		LocalConfigFile: "kmux-config.yaml",
	})
	exitOnError(err)

	// Create new Vault Source
	vault, err := kmux.NewVault("knox/microservice/secret")
	exitOnError(err)

	// Get Secrets from the vault
	secret, err := vault.GetSecret("password")
	exitOnError(err)

	log.Printf("secret: %v", secret)
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
