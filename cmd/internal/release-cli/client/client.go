package client

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/xanzy/go-gitlab"
)

type Client struct {
	client *gitlab.Client
}

func NewClient(accessToken string) *Client {
	gtlb, err := gitlab.NewClient(accessToken)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	return &Client{client: gtlb}
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
		Action:   gitlab.FileAction(gitlab.FileUpdate),
		FilePath: gitlab.String(fileName),
		Content:  gitlab.String(string(change)),
	}

	commit, _, err := g.client.Commits.CreateCommit(projectID, &gitlab.CreateCommitOptions{
		Branch:        gitlab.String(branch.Name),
		CommitMessage: &commitMessage,
		Actions:       []*gitlab.CommitActionOptions{aco},
	})
	return commit, err
}

func (g *Client) CreateMergeRequest(projectID int, sourceBranch *gitlab.Branch, description, targetBranch, title string, labels *gitlab.Labels) (*gitlab.MergeRequest, error) {
	mr, _, err := g.client.MergeRequests.CreateMergeRequest(projectID, &gitlab.CreateMergeRequestOptions{
		SourceBranch: gitlab.String(sourceBranch.Name),
		TargetBranch: &targetBranch,
		Title:        &title,
		Description:  &description,
		Squash:       gitlab.Bool(true),
		Labels:       labels,
	})
	return mr, err
}

func (g *Client) GetFile(fileName, ref string, pid int) (string, error) {
	rfo := &gitlab.GetFileOptions{
		Ref: gitlab.String(ref),
	}

	file, _, err := g.client.RepositoryFiles.GetFile(pid, fileName, rfo)
	if err != nil {
		return "", err
	}

	dec, err := base64.StdEncoding.DecodeString(file.Content)
	if err != nil {
		return "", err
	}

	f, err := ioutil.TempFile("", "tmp")
	if err != nil {
		return "", err
	}

	if _, err := f.Write(dec); err != nil {
		return "", err
	}

	f.Seek(0, 0)

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
