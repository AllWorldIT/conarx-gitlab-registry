package main

// Flaky test tracker
//
// This command runs in CI after tests. It reads the current pipeline's test
// report via the GitLab REST API, deterministically identifies failing test
// cases, and for each unique test creates or updates a tracking issue in
// GitLab. Each failure occurrence is appended as a comment, and a bucketed
// occurrence label is applied to support prioritization. When a new issue is
// created, a Slack notification is sent (if a webhook is provided).

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/cmd/internal/flake-tracker/report"
	slack "github.com/docker/distribution/cmd/internal/release-cli/slack"
	gitlab "gitlab.com/gitlab-org/api/client-go"
)

// gitlabFacade is a composite interface satisfied by our realGitLab wrapper and fakes in tests.
// It can perform both issue/note operations and fetch pipeline test reports.
type gitlabFacade interface {
	gitlabAPI
	report.PipelinesClient
}

// Factories to allow dependency injection in tests.
var (
	newGitLabClient = func(token, apiV4 string) (*gitlab.Client, error) {
		return gitlab.NewClient(token, gitlab.WithBaseURL(apiV4))
	}
	wrapGitLabClient = func(cli *gitlab.Client) gitlabFacade {
		return realGitLab{cli}
	}
	slackFactory = func(webhookURL string) slackNotifier {
		return realSlackNotifier{webhookURL: webhookURL}
	}
)

// gitlabAPI is a minimal interface over the GitLab client used for testability.
type gitlabAPI interface {
	ListProjectIssues(projectID int64, opts *gitlab.ListProjectIssuesOptions) ([]*gitlab.Issue, *gitlab.Response, error)
	CreateIssue(projectID int64, opts *gitlab.CreateIssueOptions) (*gitlab.Issue, *gitlab.Response, error)
	UpdateIssue(projectID, issueIID int64, opts *gitlab.UpdateIssueOptions) (*gitlab.Issue, *gitlab.Response, error)
	ListIssueNotes(projectID, issueIID int64, opts *gitlab.ListIssueNotesOptions) ([]*gitlab.Note, *gitlab.Response, error)
	CreateIssueNote(projectID, issueIID int64, opts *gitlab.CreateIssueNoteOptions) (*gitlab.Note, *gitlab.Response, error)
	// Pipelines
	GetPipelineTestReport(projectID, pipelineID int64, opts ...gitlab.RequestOptionFunc) (*gitlab.PipelineTestReport, *gitlab.Response, error)
}

// realGitLab wraps the official client to implement gitlabAPI.
type realGitLab struct {
	cli *gitlab.Client
}

func (r realGitLab) ListProjectIssues(projectID int64, opts *gitlab.ListProjectIssuesOptions) ([]*gitlab.Issue, *gitlab.Response, error) {
	return r.cli.Issues.ListProjectIssues(projectID, opts)
}

func (r realGitLab) CreateIssue(projectID int64, opts *gitlab.CreateIssueOptions) (*gitlab.Issue, *gitlab.Response, error) {
	return r.cli.Issues.CreateIssue(projectID, opts)
}

func (r realGitLab) UpdateIssue(projectID, issueIID int64, opts *gitlab.UpdateIssueOptions) (*gitlab.Issue, *gitlab.Response, error) {
	return r.cli.Issues.UpdateIssue(projectID, issueIID, opts)
}

func (r realGitLab) ListIssueNotes(projectID, issueIID int64, opts *gitlab.ListIssueNotesOptions) ([]*gitlab.Note, *gitlab.Response, error) {
	return r.cli.Notes.ListIssueNotes(projectID, issueIID, opts)
}

func (r realGitLab) CreateIssueNote(projectID, issueIID int64, opts *gitlab.CreateIssueNoteOptions) (*gitlab.Note, *gitlab.Response, error) {
	return r.cli.Notes.CreateIssueNote(projectID, issueIID, opts)
}

func (r realGitLab) GetPipelineTestReport(projectID, pipelineID int64, opts ...gitlab.RequestOptionFunc) (*gitlab.PipelineTestReport, *gitlab.Response, error) {
	return r.cli.Pipelines.GetPipelineTestReport(projectID, pipelineID, opts...)
}

// slackNotifier abstracts Slack notifications for testability.
type slackNotifier interface {
	Send(message string) error
}

type realSlackNotifier struct {
	webhookURL string
}

func (r realSlackNotifier) Send(message string) error {
	return slack.SendSlackNotification(r.webhookURL, message)
}

func main() {
	token := os.Getenv("FLAKE_TRACKER_GL_TOKEN")
	if token == "" {
		logf("no API token provided (FLAKE_TRACKER_GL_TOKEN), exiting")
		os.Exit(1)
	}

	// Core environment / CI variables
	webhookURL := os.Getenv("FLAKE_TRACKER_SLACK_WEBHOOK_URL")
	if webhookURL == "" {
		logf("no Slack webhook URL provided (FLAKE_TRACKER_SLACK_WEBHOOK_URL), using default webhook")
	}
	baseURL := getEnvOrDefault("CI_SERVER_URL", "https://gitlab.com")
	apiV4 := getEnvOrDefault("CI_API_V4_URL", baseURL+"/api/v4")
	projectID := mustEnv("CI_PROJECT_ID")
	workingDir := mustEnv("CI_PROJECT_DIR")
	// Convert project ID to integer for SDK usage
	projectIDInt, err := strconv.ParseInt(projectID, 10, 0)
	if err != nil {
		logf("invalid project id: %v", err)
		os.Exit(1)
	}
	// The project to create/update flaky issues in. If not provided, default to
	// the current CI project. This should be a numeric project ID.
	issueProjectID := os.Getenv("FLAKE_TRACKER_ISSUE_PROJECT_ID")
	if issueProjectID == "" {
		issueProjectID = projectID
	}
	issuePID, err := strconv.ParseInt(issueProjectID, 10, 0)
	if err != nil {
		logf("invalid project id: %v", err)
		os.Exit(1)
	}
	pipelineID := mustEnv("CI_PIPELINE_ID")
	// Convert pipeline ID to integer for SDK usage
	pipelineIDInt, err := strconv.ParseInt(pipelineID, 10, 0)
	if err != nil {
		logf("invalid pipeline id: %v", err)
		os.Exit(1)
	}
	projectURL := os.Getenv("CI_PROJECT_URL")
	// URL used in comments/notifications for quick navigation
	pipelineURL := fmt.Sprintf("%s/-/pipelines/%s", projectURL, pipelineID)
	sha := os.Getenv("CI_COMMIT_SHA")
	ref := os.Getenv("CI_COMMIT_REF_NAME")

	// official GitLab API client
	cli, err := newGitLabClient(token, apiV4)
	if err != nil {
		logf("failed to init gitlab client: %v", err)
		os.Exit(1)
	}
	gl := wrapGitLabClient(cli)

	logf("fetching pipeline test report via SDK: projectID=%d, pipelineID=%d", projectIDInt, pipelineIDInt)
	failures, err := report.FetchFailures(gl, projectIDInt, pipelineIDInt)
	if err != nil {
		logf("unable to fetch test report: %v", err)
		os.Exit(1)
	}
	// Drop parent/aggregate tests when a more specific subtest failed, and
	// also de-duplicate identical failures that normalize to the same leaf id.
	origCount := len(failures)
	failures = report.UniqueLeafFailures(failures)
	if len(failures) == 0 {
		logf("no failures found, exiting")
		return
	}
	if origCount != len(failures) {
		logf("filtered failures: %d -> %d (keeping only unique leaf subtests)", origCount, len(failures))
	}

	// Load denylist patterns (exact/glob/regex) to skip selected tests
	denylist := loadDenylistFromEnv("FLAKE_TRACKER_DENYLIST")
	// Load suite denylist to skip entire suites by name or slug
	suiteDenylist := loadDenylistFromEnv("FLAKE_TRACKER_SUITE_DENYLIST")
	// Reuse ci-tool allowfail patterns to skip whole suites
	allowfailRegexes := loadAllowfailSuitePatternsFromCI([]string{
		workingDir + "/.gitlab-ci.yml",
		workingDir + "/.gitlab/ci/integration.yml",
		workingDir + "/.gitlab/ci/test.yml",
	})
	if len(allowfailRegexes) > 0 {
		if suiteDenylist == nil {
			suiteDenylist = &testDenylist{exact: make(map[string]struct{})}
		}
		suiteDenylist.regex = append(suiteDenylist.regex, allowfailRegexes...)
		logf("loaded %d allowfail suite pattern(s) from CI config", len(allowfailRegexes))
	}

	slackNotifier := slackFactory(webhookURL)

	// Prefilter failures using denylist rules (suite and test)
	filtered := prefilterFailures(failures, denylist, suiteDenylist)
	created, updated := processFailures(gl, slackNotifier, issuePID, filtered, ref, sha, pipelineID, pipelineURL)

	logf("done. created=%d updated=%d", created, updated)
}

func prefilterFailures(in []report.Failure, denylist, suiteDenylist *testDenylist) []report.Failure {
	out := make([]report.Failure, 0, len(in))
	for _, c := range in {
		if suiteDenylist != nil {
			suiteName := strings.TrimSpace(c.Suite)
			suiteSlug := report.SuiteSlug(suiteName)
			if suiteDenylist.match(suiteName) || suiteDenylist.match(suiteSlug) {
				logf("skipping denylisted suite: %q (slug=%q)", suiteName, suiteSlug)
				continue
			}
		}
		if denylist != nil && denylist.match(c.NormalizedID()) {
			logf("skipping denylisted test: %s", c.NormalizedID())
			continue
		}
		out = append(out, c)
	}
	return out
}

// processFailures contains the core logic for processing failures into issues
// and updating existing issues with new occurrences and sending slack notifications.
// It returns the number of created and updated issues.
func processFailures(cli gitlabAPI, notifier slackNotifier, issuePID int64, failures []report.Failure, ref, sha, pipelineID, pipelineURL string) (int, int) {
	created := 0
	updated := 0
	for _, c := range failures {
		title, file, testID, occurrence := buildOccurrenceAndMeta(c, ref, sha, pipelineID, pipelineURL)

		issue, err := findIssueByExactTitle(cli, issuePID, title)
		if err != nil {
			logf("search issues failed for %q: %v", title, err)
			continue
		}
		// Base labels identifying the automation and category
		baseLabels := []string{
			"flaky::test",
			"flaky::auto",
			"Category:Container Registry",
			"backend",
			"devops::package",
			"failure::flaky-test",
			"golang",
			"group::container registry",
			"maintenance::pipelines",
			"section::ci",
			"type::maintenance",
		}
		if issue == nil {
			desc := fmt.Sprintf(
				"This issue was created automatically to track occurrences of a flaky test.\n\n"+
					"- Test: `%s`\n"+
					"- File: `%s`\n"+
					"- Ref: `%s` @ `%s`\n"+
					"- Pipeline: `%s` ([view](%s))\n"+
					"- Time: %s\n\n"+
					"Occurrences will be added as comments with details.\n",
				testID, file, ref, func() string {
					if len(sha) > 8 {
						return sha[:8]
					}
					return sha
				}(), pipelineID, pipelineURL, time.Now().UTC().Format(time.RFC3339),
			)
			if snip := strings.TrimSpace(c.Snippet()); snip != "" {
				desc += "\n- Failure snippet:\n```\n" + snip + "\n```\n"
			}
			baseLabels = append(baseLabels, bucketForCount(1))
			// Create a new issue initialized with the first bucket label
			lo := gitlab.LabelOptions(baseLabels)
			cio := &gitlab.CreateIssueOptions{
				Title:       gitlab.Ptr(title),
				Description: gitlab.Ptr(desc),
				Labels:      &lo,
				Weight:      gitlab.Ptr(int64(1)),
				// Assign to AI flake investigator https://gitlab.com/ai-container-registry-flake-investigator-gitlab-org
				// as first point of contact for automated investigation
				AssigneeIDs: gitlab.Ptr([]int64{34741781}),
			}
			is, _, err := cli.CreateIssue(issuePID, cio)
			if err != nil {
				logf("create issue failed for %q: %v", title, err)
				continue
			}
			if _, _, err := cli.CreateIssueNote(issuePID, is.IID, &gitlab.CreateIssueNoteOptions{Body: gitlab.Ptr(occurrence)}); err != nil {
				logf("failed to add occurrence note to issue #%d: %v", is.IID, err)
			}
			created++
			logf("created issue #%d for %s", is.IID, testID)
			// Slack notification (best-effort, non-blocking)
			if notifier != nil {
				msg := fmt.Sprintf(
					":warning: New flaky test detected: `%s`\nIssue: %s\nPipeline: %s",
					testID, is.WebURL, pipelineURL,
				)
				if err := notifier.Send(msg); err != nil {
					logf("slack notify failed: %v", err)
				} else {
					logf("slack notification sent for %s", testID)
				}
			}
			continue
		}
		// If the matching issue is closed, reopen it to make it visible again
		if strings.EqualFold(issue.State, "closed") {
			if _, _, err := cli.UpdateIssue(issuePID, issue.IID, &gitlab.UpdateIssueOptions{
				StateEvent: gitlab.Ptr("reopen"),
			}); err != nil {
				logf("failed to reopen issue #%d: %v", issue.IID, err)
			}
		}
		prev, err := countOccurrences(cli, issuePID, issue.IID)
		if err != nil {
			logf("counting occurrences failed for !%d: %v", issue.IID, err)
			continue
		}
		newCount := prev + 1
		// Replace any previous occurrence bucket with the current total bucket
		labels := withoutOccurrenceBuckets(issue.Labels)
		labels = append(labels, bucketForCount(newCount))
		ulo := gitlab.LabelOptions(labels)
		if _, _, err := cli.UpdateIssue(issuePID, issue.IID, &gitlab.UpdateIssueOptions{Labels: &ulo, Weight: gitlab.Ptr(newCount)}); err != nil {
			logf("failed to update labels for issue #%d: %v", issue.IID, err)
		}
		if _, _, err := cli.CreateIssueNote(issuePID, issue.IID, &gitlab.CreateIssueNoteOptions{Body: gitlab.Ptr(occurrence)}); err != nil {
			logf("failed to add occurrence note to issue #%d: %v", issue.IID, err)
		}
		updated++
		logf("updated issue #%d for %s (occurrences=%d)", issue.IID, testID, newCount)
	}
	return created, updated
}

// buildOccurrenceAndMeta builds the occurrence and metadata for a failure.
func buildOccurrenceAndMeta(c report.Failure, ref, sha, pipelineID, pipelineURL string) (title, file, testID, occurrence string) {
	testID = c.NormalizedID()
	title = "[Flaky] " + testID
	file = c.File
	now := time.Now().UTC()
	shortSHA := sha
	if len(shortSHA) > 8 {
		shortSHA = shortSHA[:8]
	}
	occurrence = fmt.Sprintf(
		"FLAKE_OCCURRENCE:\n"+
			"- Test: `%s`\n"+
			"- File: `%s`\n"+
			"- Ref: `%s` @ `%s`\n"+
			"- Pipeline: `%s` ([view](%s))\n"+
			"- Time: %s",
		testID, file, ref, shortSHA, pipelineID, pipelineURL, now.Format(time.RFC3339),
	)
	if snip := strings.TrimSpace(c.Snippet()); snip != "" {
		occurrence += "\n- Failure snippet:\n```\n" + snip + "\n```"
	}
	return title, file, testID, occurrence
}

// getEnvOrDefault returns the environment variable value if set, otherwise def.
func getEnvOrDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// mustEnv returns the env var value or panics if unset. Used for core CI vars
// that must be present for the command to function.
func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic(fmt.Sprintf("missing required env var: %s", name))
	}
	return v
}

// logf emits a namespaced log line to stdout.
func logf(format string, args ...any) {
	fmt.Printf("[flaky-tracker] "+format+"\n", args...)
}

// bucketForCount maps an occurrence count to a label used for triage buckets.
func bucketForCount(n int64) string {
	switch {
	case n <= 3:
		return "flaky-occurrences::1-3"
	case n <= 10:
		return "flaky-occurrences::4-10"
	case n <= 25:
		return "flaky-occurrences::11-25"
	default:
		return "flaky-occurrences::>25"
	}
}

// findIssueByExactTitle searches issues by title in the target project and
// returns an exact-match issue if found (opened first, then closed).
func findIssueByExactTitle(cli gitlabAPI, projectID int64, title string) (*gitlab.Issue, error) {
	if is, err := searchTitleByState(cli, projectID, title, "opened"); err != nil || is != nil {
		return is, err
	}
	return searchTitleByState(cli, projectID, title, "closed")
}

// searchTitleByState searches issues by title in the target project and returns an exact-match issue if found.
func searchTitleByState(cli gitlabAPI, projectID int64, title, state string) (*gitlab.Issue, error) {
	var page int64 = 1
	for {
		issues, resp, err := cli.ListProjectIssues(projectID, &gitlab.ListProjectIssuesOptions{
			Search: gitlab.Ptr(title),
			ListOptions: gitlab.ListOptions{
				PerPage: 100,
				Page:    page,
			},
			State: gitlab.Ptr(state),
		})
		if err != nil {
			return nil, err
		}
		for _, is := range issues {
			if is.Title == title {
				return is, nil
			}
		}
		if resp.CurrentPage >= resp.TotalPages || len(issues) == 0 {
			return nil, nil
		}
		page++
	}
}

// countOccurrences counts how many tracker comments were previously added to
// the issue by scanning for the "FLAKE_OCCURRENCE:" marker.
func countOccurrences(cli gitlabAPI, projectID, issueIID int64) (int64, error) {
	var page int64 = 1
	var total int64
	for {
		notes, resp, err := cli.ListIssueNotes(projectID, issueIID, &gitlab.ListIssueNotesOptions{
			ListOptions: gitlab.ListOptions{PerPage: 100, Page: page},
		})
		if err != nil {
			return 0, err
		}
		for _, n := range notes {
			if strings.Contains(n.Body, "FLAKE_OCCURRENCE:") {
				total++
			}
		}
		if resp.CurrentPage >= resp.TotalPages || len(notes) == 0 {
			break
		}
		page++
	}
	return total, nil
}

// withoutOccurrenceBuckets removes any existing occurrence bucket labels to
// allow applying the correct bucket for the latest total count.
func withoutOccurrenceBuckets(labels []string) []string {
	out := make([]string, 0, len(labels))
	for _, l := range labels {
		if !strings.HasPrefix(l, "flaky-occurrences::") {
			out = append(out, l)
		}
	}
	return out
}

// testDenylist stores matchers for exact, glob and regex patterns.
type testDenylist struct {
	exact map[string]struct{}
	globs []string
	regex []*regexp.Regexp
}

func (b *testDenylist) match(s string) bool {
	if b == nil {
		return false
	}
	if _, ok := b.exact[s]; ok {
		return true
	}
	for _, g := range b.globs {
		if ok, _ := path.Match(g, s); ok {
			return true
		}
	}
	for _, r := range b.regex {
		if r.MatchString(s) {
			return true
		}
	}
	return false
}

// loadDenylistFromEnv loads denylist patterns from the environment variable.
// Patterns:
// - /regex/ are treated as regular expressions
// - patterns containing *, ?, [ are treated as globs
// - otherwise treated as exact string matches
func loadDenylistFromEnv(envVar string) *testDenylist {
	var items []string
	if raw := strings.TrimSpace(os.Getenv(envVar)); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			items = append(items, strings.TrimSpace(p))
		}
	}

	if len(items) == 0 {
		return nil
	}
	bl := &testDenylist{
		exact: make(map[string]struct{}),
	}
	for _, p := range items {
		if p == "" {
			continue
		}
		// /regex/ form
		if len(p) >= 2 && strings.HasPrefix(p, "/") && strings.HasSuffix(p, "/") {
			re, err := regexp.Compile(p[1 : len(p)-1])
			if err != nil {
				logf("invalid denylist regex %q: %v", p, err)
				continue
			}
			bl.regex = append(bl.regex, re)
			continue
		}
		// glob
		if strings.ContainsAny(p, "*?[") {
			bl.globs = append(bl.globs, p)
			continue
		}
		// exact
		bl.exact[p] = struct{}{}
	}
	return bl
}

// loadAllowfailSuitePatternsFromCI scans provided CI YAML files for lines in the form:
//
//	# ci-tool-allowfail <regex>
//
// and returns compiled regex patterns to be used for skipping whole suites.
func loadAllowfailSuitePatternsFromCI(paths []string) []*regexp.Regexp {
	patterns := make([]*regexp.Regexp, 0, 4)
	commentRe := regexp.MustCompile(`(?i)#\s*ci-tool-allowfail\s+(.+)$`)

	for _, p := range paths {
		// #nosec G304 -- file paths are repo-controlled CI YAML files
		data, err := os.ReadFile(p)
		if err != nil {
			// Ignore missing files; report other IO errors for visibility
			if !errors.Is(err, fs.ErrNotExist) {
				logf("warning: failed reading %s: %v", p, err)
			}
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimRight(line, "\r")
			m := commentRe.FindStringSubmatch(line)
			if len(m) != 2 {
				continue
			}
			raw := strings.TrimSpace(m[1])
			if raw == "" {
				continue
			}
			re, err := regexp.Compile(raw)
			if err != nil {
				logf("invalid ci-tool-allowfail regex %q in %s: %v", raw, p, err)
				continue
			}
			patterns = append(patterns, re)
		}
	}
	return patterns
}
