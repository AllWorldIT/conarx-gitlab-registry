package cmd

import "github.com/spf13/cobra"

var RegistryToken string

var rootCmd = &cobra.Command{
	Use:   "release",
	Short: "A CLI tool for Container Registry releases",
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVarP(&RegistryToken, "registry-access-token", "", "", "Registry Access Token")
	rootCmd.MarkFlagRequired("registry-access-token")
}
