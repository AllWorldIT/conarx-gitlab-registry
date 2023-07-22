package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const versionPlaceholder = "{VERSION}"

type Release struct {
	ProjectID     int      `mapstructure:"project_id"`
	BranchName    string   `mapstructure:"branch_name"`
	CommitMessage string   `mapstructure:"commit_message"`
	Ref           string   `mapstructure:"ref"`
	MRTitle       string   `mapstructure:"mr_title"`
	Paths         []string `mapstructure:"paths"`
}

func (r *Release) String() string {
	return fmt.Sprintf("Project ID: %d\nBranch Name: %s\nCommit Message: %s\nRef: %s\nMR Title: %snPaths: %s",
		r.ProjectID, r.BranchName, r.CommitMessage, r.Ref, r.MRTitle, strings.Join(r.Paths, ", "))
}

func initConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath("cmd/internal/release-cli/config")
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func readConfig(cmd, version string) (*Release, error) {
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	var release Release
	key := fmt.Sprintf("releases.%s", cmd)
	err = viper.UnmarshalKey(key, &release)
	if err != nil {
		return nil, err
	}

	// replace version placeholder in branch name, commit message and MR title (if applicable)
	release.BranchName = strings.ReplaceAll(release.BranchName, versionPlaceholder, version)
	release.CommitMessage = strings.ReplaceAll(release.CommitMessage, versionPlaceholder, version)
	release.MRTitle = strings.ReplaceAll(release.MRTitle, versionPlaceholder, version)

	return &release, nil
}
