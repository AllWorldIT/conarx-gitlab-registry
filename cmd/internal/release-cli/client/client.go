package client

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"

	gitlab "gitlab.com/gitlab-org/api/client-go"
)

type Client struct {
	client *gitlab.Client
}

func NewClient(accessToken string) (*Client, error) {
	gtlb, err := gitlab.NewClient(accessToken)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &Client{client: gtlb}, nil
}

func (g *Client) CreateBranch(projectID int, branchName, ref string) (*gitlab.Branch, error) {
	branch, _, err := g.client.Branches.CreateBranch(projectID, &gitlab.CreateBranchOptions{
		Branch: &branchName,
		Ref:    &ref,
	})
	return branch, err
}

func (g *Client) CreateCommit(projectID int, change []byte, fileName, commitMessage string, branch *gitlab.Branch) (*gitlab.Commit, error) {
	aco := &gitlab.CommitActionOptions{
		Action:   gitlab.Ptr(gitlab.FileUpdate),
		FilePath: gitlab.Ptr(fileName),
		Content:  gitlab.Ptr(string(change)),
	}

	commit, _, err := g.client.Commits.CreateCommit(projectID, &gitlab.CreateCommitOptions{
		Branch:        gitlab.Ptr(branch.Name),
		CommitMessage: &commitMessage,
		Actions:       []*gitlab.CommitActionOptions{aco},
	})
	return commit, err
}

func (g *Client) CreateMergeRequest(projectID int, sourceBranch *gitlab.Branch, description, targetBranch, title string, labels *gitlab.LabelOptions, reviwerIDs []int) (*gitlab.MergeRequest, error) {
	mr, _, err := g.client.MergeRequests.CreateMergeRequest(projectID, &gitlab.CreateMergeRequestOptions{
		SourceBranch: gitlab.Ptr(sourceBranch.Name),
		TargetBranch: &targetBranch,
		Title:        &title,
		Description:  &description,
		Squash:       gitlab.Ptr(true),
		Labels:       labels,
		ReviewerIDs:  &reviwerIDs,
	})
	return mr, err
}

func (g *Client) GetFile(fileName, ref string, pid int) (string, error) {
	rfo := &gitlab.GetFileOptions{
		Ref: gitlab.Ptr(ref),
	}

	file, _, err := g.client.RepositoryFiles.GetFile(pid, fileName, rfo)
	if err != nil {
		return "", err
	}

	dec, err := base64.StdEncoding.DecodeString(file.Content)
	if err != nil {
		return "", err
	}

	f, err := os.CreateTemp("", "tmp")
	if err != nil {
		return "", err
	}

	if _, err := f.Write(dec); err != nil {
		return "", err
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return "", fmt.Errorf("file seek: %w", err)
	}

	return f.Name(), err
}

func (g *Client) SendRequestToDeps(projectID int, triggerToken, ref string) (string, error) {
	rpto := &gitlab.RunPipelineTriggerOptions{
		Ref:       &ref,
		Token:     &triggerToken,
		Variables: map[string]string{"DEPS_PIPELINE": "true"},
	}

	pipeline, _, err := g.client.PipelineTriggers.RunPipelineTrigger(projectID, rpto)
	if err != nil {
		return "", err
	}

	return pipeline.WebURL, nil
}

func (g *Client) GetChangelog(version string) (string, error) {
	projectID, err := strconv.Atoi(os.Getenv("CI_PROJECT_ID"))
	if err != nil {
		return "", err
	}

	releases, _, err := g.client.Releases.ListReleases(projectID, nil)
	if err != nil {
		return "", err
	}

	for _, release := range releases {
		if release.TagName == version {
			return release.Description, nil
		}
	}

	return "", fmt.Errorf("release with version %s not found", version)
}

func (g *Client) BranchExists(projectID int, branchName string) (bool, error) {
	_, _, err := g.client.Branches.GetBranch(projectID, branchName)
	if err != nil {
		var errResp *gitlab.ErrorResponse
		if errors.As(err, &errResp) && errResp.Response.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (g *Client) MergeRequestExistsByPattern(projectID int, pattern *regexp.Regexp) (bool, error) {
	state := "opened"
	search := "Update gitlab-org/container-registry"
	opts := &gitlab.ListProjectMergeRequestsOptions{
		Search: &search,
		State:  &state,
	}

	mrs, _, err := g.client.MergeRequests.ListProjectMergeRequests(projectID, opts)
	if err != nil {
		return false, err
	}

	for _, mr := range mrs {
		if pattern.MatchString(mr.Title) {
			return true, nil
		}
	}

	return false, nil
}
