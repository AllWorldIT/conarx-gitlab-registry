package systemops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	compute "cloud.google.com/go/compute/metadata"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"go.uber.org/zap"
)

const (
	// Maximum amount of time all operations in this stage may take
	defaultVMLabelsInspectionTimeout = 15 * time.Second

	// VM metadata attribute names
	vmLabelRegistryBranchName = "registry_branch"
	vmLabelGitLabBranchName   = "gitlab_branch"
	vmLabelDeploymentType     = "deployment_type"
	vmLabelEEVersion          = "ee_version"
	vmLabelNoDeploy           = "no_deploy"

	// Go to project page of the Registry/GitLab repos on gitlab.com to verify/update these:
	RegistryProjectID = 13831684
	GitLabProjectID   = 278964
)

// VMLabelsCache holds cached data from VM labels inspection
type VMLabelsCache struct {
	DeploymentType   string `json:"deployment_type"`
	EEVersion        bool   `json:"ee_version"`
	NoDeploy         bool   `json:"no_deploy"`
	RegistryBranch   string `json:"registry_branch"`
	RegistryCommitID string `json:"registry_commit_id"`
	GitLabCommitID   string `json:"gitlab_commit_id"`
}

// VMLabels contains all metadata labels read from the VM instance.
type VMLabels struct {
	DeploymentType   string
	EEVersion        bool
	NoDeploy         bool
	RegistryBranch   string
	RegistryCommitID string
	GitLabCommitID   string
}

// inspectVMLabels reads VM metadata from GCE instance metadata service.
// It retrieves deployment configuration and resolves branch names to commit IDs.
// Results are cached to ensure consistent behavior across restarts.
func inspectVMLabels(logger *zap.SugaredLogger) (VMLabels, status.PartialStatus) {
	logger.Info("Reading VM metadata from GCE metadata service")

	// Try to load from cache first
	cached, err := loadVMLabelsCache(logger, constants.DefaultLabelsCacheFilePath)
	if err == nil {
		logger.Infow("Using cached VM labels",
			"deploymentType", cached.DeploymentType,
			"eeVersion", cached.EEVersion,
			"noDeploy", cached.NoDeploy,
			"registryBranch", cached.RegistryBranch,
			"registryCommitID", cached.RegistryCommitID,
			"gitLabCommitID", cached.GitLabCommitID,
		)
		return VMLabels{
				DeploymentType:   cached.DeploymentType,
				EEVersion:        cached.EEVersion,
				NoDeploy:         cached.NoDeploy,
				RegistryBranch:   cached.RegistryBranch,
				RegistryCommitID: cached.RegistryCommitID,
				GitLabCommitID:   cached.GitLabCommitID,
			}, status.PartialStatus{
				Succeeded: status.StatusStageOK,
				DetailedMessage: []string{
					fmt.Sprintf("Deployment type (cached): %s", cached.DeploymentType),
					fmt.Sprintf("EE version (cached): %t", cached.EEVersion),
					fmt.Sprintf("No deploy (cached): %t", cached.NoDeploy),
					fmt.Sprintf("Registry branch (cached): %s", cached.RegistryBranch),
					fmt.Sprintf("Registry commit ID (cached): %s", cached.RegistryCommitID),
					fmt.Sprintf("GitLab commit ID (cached): %s", cached.GitLabCommitID),
				},
				Metadata: map[string]string{
					"deployment_type":    cached.DeploymentType,
					"ee_version":         fmt.Sprintf("%t", cached.EEVersion),
					"no_deploy":          fmt.Sprintf("%t", cached.NoDeploy),
					"registry_branch":    cached.RegistryBranch,
					"registry_commit_id": cached.RegistryCommitID,
					"gitlab_commit_id":   cached.GitLabCommitID,
					"from_cache":         "true",
				},
			}
	}

	if !errors.Is(err, os.ErrNotExist) {
		return VMLabels{}, status.Failed("unable to read cache file: %v", err)
	}

	// Cache doesn't exist, fetch from metadata
	ctx, cancel := context.WithTimeout(context.Background(), defaultVMLabelsInspectionTimeout)
	defer cancel()

	// Read deployment_type from instance metadata
	deploymentType, err := compute.GetWithContext(ctx, "instance/attributes/"+vmLabelDeploymentType)
	if err != nil {
		return VMLabels{}, status.Failed("unable to read deployment_type from VM metadata: %v", err)
	}

	// Validate deployment type
	if deploymentType != "charts" && deploymentType != "omnibus" {
		return VMLabels{}, status.Failed(
			"invalid deployment type %q from VM metadata: must be 'charts' or 'omnibus'",
			deploymentType,
		)
	}

	// Read ee_version from instance metadata
	eeVersionStr, err := compute.GetWithContext(ctx, "instance/attributes/"+vmLabelEEVersion)
	if err != nil {
		return VMLabels{}, status.Failed("unable to read ee_version from VM metadata: %v", err)
	}
	eeVersion := eeVersionStr == "true"

	// Read no_deploy from instance metadata
	noDeployStr, err := compute.GetWithContext(ctx, "instance/attributes/"+vmLabelNoDeploy)
	if err != nil {
		return VMLabels{}, status.Failed("unable to read no_deploy from VM metadata: %v", err)
	}
	noDeploy := noDeployStr == "true"

	// Read registry_branch from VM metadata
	registryBranch, err := compute.GetWithContext(ctx, "instance/attributes/"+vmLabelRegistryBranchName)
	if err != nil {
		return VMLabels{}, status.Failed("unable to fetch registry branch name from GCE metadata: %v", err)
	}
	logger.Infof("registry branch name: %s", registryBranch)

	// Read gitlab_branch from VM metadata
	gitLabBranch, err := compute.GetWithContext(ctx, "instance/attributes/"+vmLabelGitLabBranchName)
	if err != nil {
		return VMLabels{}, status.Failed("unable to fetch GitLab branch name from GCE metadata: %v", err)
	}
	logger.Infof("gitlab branch name: %s", gitLabBranch)

	// Resolve registry branch to commit ID
	registryCommitID, err := resolveBranchToCommitID(ctx, logger, RegistryProjectID, registryBranch, "registry")
	if err != nil {
		return VMLabels{}, status.Failed(
			"unable to resolve registry branch %q to commit ID: %v", registryBranch, err,
		)
	}

	// Resolve GitLab branch to commit ID
	gitLabCommitID, err := resolveBranchToCommitID(ctx, logger, GitLabProjectID, gitLabBranch, "gitlab")
	if err != nil {
		return VMLabels{}, status.Failed(
			"unable to resolve GitLab branch %q to commit ID: %v", gitLabBranch, err,
		)
	}

	// Save to cache
	err = saveVMLabelsCache(logger, constants.DefaultLabelsCacheFilePath, VMLabelsCache{
		DeploymentType:   deploymentType,
		EEVersion:        eeVersion,
		NoDeploy:         noDeploy,
		RegistryBranch:   registryBranch,
		RegistryCommitID: registryCommitID,
		GitLabCommitID:   gitLabCommitID,
	})
	if err != nil {
		return VMLabels{}, status.Failed("unable to save VM labels cache: %v", err)
	}

	logger.Infow("Successfully read VM metadata",
		"deploymentType", deploymentType,
		"eeVersion", eeVersion,
		"noDeploy", noDeploy,
		"registryBranch", registryBranch,
		"registryCommitID", registryCommitID,
		"gitLabBranch", gitLabBranch,
		"gitLabCommitID", gitLabCommitID,
	)

	return VMLabels{
			DeploymentType:   deploymentType,
			EEVersion:        eeVersion,
			NoDeploy:         noDeploy,
			RegistryBranch:   registryBranch,
			RegistryCommitID: registryCommitID,
			GitLabCommitID:   gitLabCommitID,
		}, status.PartialStatus{
			Succeeded: status.StatusStageOK,
			DetailedMessage: []string{
				fmt.Sprintf("Deployment type: %s", deploymentType),
				fmt.Sprintf("EE version: %t", eeVersion),
				fmt.Sprintf("No deploy: %t", noDeploy),
				fmt.Sprintf("Registry branch: %s", registryBranch),
				fmt.Sprintf("Registry commit ID: %s", registryCommitID),
				fmt.Sprintf("GitLab branch: %s", gitLabBranch),
				fmt.Sprintf("GitLab commit ID: %s", gitLabCommitID),
				"ATTENTION: Subsequent pushes to branches will be ignored.",
				"ATTENTION: Please re-create the dev vm if you would like to update the references.",
			},
			Metadata: map[string]string{
				"deployment_type":    deploymentType,
				"ee_version":         eeVersionStr,
				"no_deploy":          noDeployStr,
				"registry_branch":    registryBranch,
				"registry_commit_id": registryCommitID,
				"gitlab_branch":      gitLabBranch,
				"gitlab_commit_id":   gitLabCommitID,
				"from_cache":         "false",
			},
		}
}

func loadVMLabelsCache(logger *zap.SugaredLogger, path string) (*VMLabelsCache, error) {
	logger.Infof("retrieving cached VM labels data from %s", path)
	// nolint: gosec // there is nothing sensitive here
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("unable to read cache file %s: %w", path, err)
	}

	cache := new(VMLabelsCache)
	err = json.Unmarshal(b, cache)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal cache: %w", err)
	}

	return cache, nil
}

func saveVMLabelsCache(logger *zap.SugaredLogger, path string, cache VMLabelsCache) error {
	logger.Infof("storing VM labels data in %s", path)

	b, err := json.Marshal(cache)
	if err != nil {
		return fmt.Errorf("unable to marshal cache: %w", err)
	}

	// nolint: gosec // this file needs to be readable
	err = os.WriteFile(path, b, 0o644)
	if err != nil {
		return fmt.Errorf("unable to write marshaled cache to %s: %w", path, err)
	}

	return nil
}

// resolveBranchToCommitID resolves a branch specification to a commit ID.
// The branchSpec can be:
// - A plain branch name (e.g., "master") - will be resolved via GitLab API
// - A commit:<sha> format (e.g., "commit:abc123") - will use the SHA directly
func resolveBranchToCommitID(
	ctx context.Context,
	logger *zap.SugaredLogger,
	projectID int,
	branchSpec, repoName string,
) (string, error) {
	// Check if it's a direct commit reference
	if strings.HasPrefix(branchSpec, "commit:") {
		commitID := strings.TrimPrefix(branchSpec, "commit:")
		logger.Infof("%s using direct commit ID: %s", repoName, commitID)
		return commitID, nil
	}

	// It's a branch name, resolve via API
	commitID, err := getBranchCommitID(ctx, logger, projectID, branchSpec)
	if err != nil {
		return "", fmt.Errorf("unable to fetch commit ID for %s branch %s: %w", repoName, branchSpec, err)
	}
	logger.Infof("%s commit ID: %s", repoName, commitID)

	return commitID, nil
}

func getBranchCommitID(
	ctx context.Context,
	logger *zap.SugaredLogger,
	projectID int,
	branchName string,
) (string, error) {
	apiURL := fmt.Sprintf(
		"https://gitlab.com/api/v4/projects/%d/repository/branches/%s",
		projectID, url.QueryEscape(branchName),
	)

	resp, statusCode, err := httpGetReq(ctx, logger, apiURL, nil, false)
	if err != nil {
		return "", fmt.Errorf("http GET request failed: %w", err)
	}
	if statusCode != http.StatusOK {
		return "", fmt.Errorf("http GET request returned status %d", statusCode)
	}

	var result struct {
		Commit struct {
			ID string `json:"id"`
		} `json:"commit"`
	}

	err = json.Unmarshal(resp, &result)
	if err != nil {
		return "", fmt.Errorf("unable to unmarshal response body: %w", err)
	}

	return result.Commit.ID, nil
}
