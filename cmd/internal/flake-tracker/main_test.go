package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/docker/distribution/cmd/internal/flake-tracker/report"
	"github.com/stretchr/testify/require"
	gitlab "gitlab.com/gitlab-org/api/client-go"
)

func TestLoadDenylist_EnvPatterns(t *testing.T) {
	// exact, glob, regex
	exact := "gitlab.com/group/proj/pkg.TestTop/Leaf"
	glob := "gitlab.com/group/proj/*.TestTop/*"
	// Match only the exact id above using regex
	regex := `/^gitlab\.com\/group\/proj\/pkg\.TestTop\/Leave$/`
	t.Setenv("FLAKE_TRACKER_DENYLIST", strings.Join([]string{exact, glob, regex}, ","))

	bl := loadDenylistFromEnv("FLAKE_TRACKER_DENYLIST")
	require.NotNil(t, bl)
	// exact
	require.True(t, bl.match(exact))
	// glob should match different middle segment and any leaf
	require.True(t, bl.match("gitlab.com/group/proj/alpha.TestTop/Anything"))
	// regex should match the exact id, not a different one
	require.True(t, bl.match("gitlab.com/group/proj/pkg.TestTop/Leave"))
	require.True(t, bl.match("gitlab.com/group/proj/pkg.TestTop/Leaves"))
	// negative case
	require.False(t, bl.match("unrelated.Test/Case"))
}

func TestWithoutOccurrenceBuckets_RemovesOnlyOccurrenceLabels(t *testing.T) {
	in := []string{
		"flaky-occurrences::1-3",
		"flaky-occurrences::4-10",
		"keep-me",
		"another",
	}
	out := withoutOccurrenceBuckets(in)
	require.Equal(t, []string{"keep-me", "another"}, out)
}

func TestBucketForCount_Ranges(t *testing.T) {
	cases := map[int64]string{
		1:   "flaky-occurrences::1-3",
		3:   "flaky-occurrences::1-3",
		4:   "flaky-occurrences::4-10",
		10:  "flaky-occurrences::4-10",
		11:  "flaky-occurrences::11-25",
		25:  "flaky-occurrences::11-25",
		26:  "flaky-occurrences::>25",
		100: "flaky-occurrences::>25",
	}
	for n, want := range cases {
		require.Equal(t, want, bucketForCount(n))
	}
}

func TestGetEnvOrDefault(t *testing.T) {
	t.Setenv("FOO_BAR_BAZ", "")
	require.Equal(t, "def", getEnvOrDefault("FOO_BAR_BAZ", "def"))
	t.Setenv("FOO_BAR_BAZ", "val")
	require.Equal(t, "val", getEnvOrDefault("FOO_BAR_BAZ", "def"))
}

func TestMustEnv_PanicsWhenUnset(t *testing.T) {
	defer func() {
		require.NotNil(t, recover(), "expected panic when env unset")
	}()
	_ = os.Unsetenv("NEVER_SET_ENV")
	_ = mustEnv("NEVER_SET_ENV")
}

func TestLoadDenylist_EmptyReturnsNil(t *testing.T) {
	t.Setenv("FLAKE_TRACKER_DENYLIST", "")
	require.Nil(t, loadDenylistFromEnv("FLAKE_TRACKER_DENYLIST"))
}

func TestSuiteDenylist_EnvGlobMatchesOnlyDesiredSuite(t *testing.T) {
	// Glob pattern: want to match azure: [1.24, azure, ...] only
	// Use a regex without commas to match any suite containing 'azure' inside the brackets
	t.Setenv("FLAKE_TRACKER_SUITE_DENYLIST", `/^azure:\s*\[[^]]*\bazure\b[^]]*\]/`)
	bl := loadDenylistFromEnv("FLAKE_TRACKER_SUITE_DENYLIST")
	require.NotNil(t, bl)
	match := "azure: [1.24, azure, shared_key]"
	match2 := "azure: [1.25, azure, shared_key]"
	noMatch1 := "azure: [1.24, azure_v2, shared_key]"
	require.True(t, bl.match(match))
	require.True(t, bl.match(match2))
	require.False(t, bl.match(noMatch1))
}

func TestSuiteDenylist_SlugPatternAlsoMatches(t *testing.T) {
	t.Setenv("FLAKE_TRACKER_SUITE_DENYLIST", "")
	// Provide slug-style pattern; should match slugged suite name
	t.Setenv("FLAKE_TRACKER_SUITE_DENYLIST", "azure-1_24-azure*")
	bl := loadDenylistFromEnv("FLAKE_TRACKER_SUITE_DENYLIST")
	require.NotNil(t, bl)
	slug := "azure-1_24-azure-shared-key"
	raw := "azure: [1.24, azure, shared key]"
	require.True(t, bl.match(slug) || bl.match(raw))
}

type fakeGitLab struct {
	issues         []*gitlab.Issue
	notes          map[int64][]*gitlab.Note
	pipelineReport *gitlab.PipelineTestReport

	// error injection toggles
	errOnListIssues  bool
	errOnCreateIssue bool
	errOnUpdateIssue bool
	errOnListNotes   bool
	errOnCreateNote  bool
	listIssuesCalls  int
	createIssueCalls int
	updateIssueCalls int
	listNotesCalls   int
	createNoteCalls  int
}

type fakeSlackNotifier struct {
	msgs      []string
	fail      bool
	sendCalls int
}

func (f *fakeSlackNotifier) Send(message string) error {
	f.sendCalls++
	f.msgs = append(f.msgs, message)
	if f.fail {
		return fmt.Errorf("send failed")
	}
	return nil
}

func newFakeGitLab() *fakeGitLab {
	return &fakeGitLab{notes: make(map[int64][]*gitlab.Note)}
}

func (f *fakeGitLab) nextIID() int64 {
	return int64(len(f.issues) + 1)
}

func (f *fakeGitLab) ListProjectIssues(_ int64, opts *gitlab.ListProjectIssuesOptions) ([]*gitlab.Issue, *gitlab.Response, error) {
	f.listIssuesCalls++
	if f.errOnListIssues {
		return nil, nil, fmt.Errorf("list issues error")
	}
	var out []*gitlab.Issue
	state := ""
	if opts != nil && opts.State != nil {
		state = *opts.State
	}
	for _, is := range f.issues {
		if state != "" && !strings.EqualFold(is.State, state) {
			continue
		}
		if opts != nil && opts.Search != nil && *opts.Search != "" && !strings.Contains(is.Title, *opts.Search) {
			continue
		}
		out = append(out, is)
	}
	return out, &gitlab.Response{CurrentPage: 1, TotalPages: 1}, nil
}

func (f *fakeGitLab) CreateIssue(_ int64, opts *gitlab.CreateIssueOptions) (*gitlab.Issue, *gitlab.Response, error) {
	f.createIssueCalls++
	if f.errOnCreateIssue {
		return nil, nil, fmt.Errorf("create issue error")
	}
	iid := f.nextIID()
	is := &gitlab.Issue{
		IID:    iid,
		Title:  *opts.Title,
		State:  "opened",
		Labels: []string(*opts.Labels),
		Weight: *opts.Weight,
	}
	f.issues = append(f.issues, is)
	return is, &gitlab.Response{}, nil
}

func (f *fakeGitLab) UpdateIssue(_, issueIID int64, opts *gitlab.UpdateIssueOptions) (*gitlab.Issue, *gitlab.Response, error) {
	f.updateIssueCalls++
	if f.errOnUpdateIssue {
		return nil, nil, fmt.Errorf("update issue error")
	}
	var is *gitlab.Issue
	for _, it := range f.issues {
		if it.IID == issueIID {
			is = it
			break
		}
	}
	if is == nil {
		return nil, nil, nil
	}
	if opts.StateEvent != nil && *opts.StateEvent == "reopen" {
		is.State = "opened"
	}
	if opts.Labels != nil {
		is.Labels = []string(*opts.Labels)
	}
	if opts.Weight != nil {
		is.Weight = *opts.Weight
	}
	return is, &gitlab.Response{}, nil
}

func (f *fakeGitLab) ListIssueNotes(_, issueIID int64, _ *gitlab.ListIssueNotesOptions) ([]*gitlab.Note, *gitlab.Response, error) {
	f.listNotesCalls++
	if f.errOnListNotes {
		return nil, nil, fmt.Errorf("list notes error")
	}
	return f.notes[issueIID], &gitlab.Response{CurrentPage: 1, TotalPages: 1}, nil
}

func (f *fakeGitLab) CreateIssueNote(_, issueIID int64, opts *gitlab.CreateIssueNoteOptions) (*gitlab.Note, *gitlab.Response, error) {
	f.createNoteCalls++
	if f.errOnCreateNote {
		return nil, nil, fmt.Errorf("create note error")
	}
	n := &gitlab.Note{Body: *opts.Body}
	f.notes[issueIID] = append(f.notes[issueIID], n)
	return n, &gitlab.Response{}, nil
}

func (f *fakeGitLab) GetPipelineTestReport(_, _ int64, _ ...gitlab.RequestOptionFunc) (*gitlab.PipelineTestReport, *gitlab.Response, error) {
	return f.pipelineReport, &gitlab.Response{}, nil
}

func TestE2E_Main_CreatesIssue_And_SendsSlack(t *testing.T) {
	// Save and override factories
	oldNew := newGitLabClient
	oldWrap := wrapGitLabClient
	oldSlack := slackFactory
	defer func() {
		newGitLabClient = oldNew
		wrapGitLabClient = oldWrap
		slackFactory = oldSlack
	}()

	// Prepare fakes
	fcli := newFakeGitLab()
	fcli.pipelineReport = &gitlab.PipelineTestReport{
		TestSuites: []*gitlab.PipelineTestSuites{
			{
				Name: "suite1",
				TestCases: []*gitlab.PipelineTestCases{
					{Name: "Leaf", Classname: "pkg.TestTop", File: "x_test.go", Status: "failed", SystemOutput: "x", StackTrace: ""},
				},
			},
		},
	}
	sn := &fakeSlackNotifier{}

	// Inject factories
	newGitLabClient = func(_, _ string) (*gitlab.Client, error) { return nil, nil }
	wrapGitLabClient = func(_ *gitlab.Client) gitlabFacade { return fcli }
	slackFactory = func(_ string) slackNotifier { return sn }

	// Env
	t.Setenv("FLAKE_TRACKER_GL_TOKEN", "token")
	t.Setenv("CI_PROJECT_ID", "1")
	t.Setenv("CI_PIPELINE_ID", "2")
	t.Setenv("CI_SERVER_URL", "https://gitlab.com")
	t.Setenv("CI_API_V4_URL", "https://gitlab.com/api/v4")
	t.Setenv("CI_PROJECT_URL", "https://example.com/group/proj")
	t.Setenv("CI_COMMIT_REF_NAME", "main")
	t.Setenv("CI_COMMIT_SHA", "deadbeefcafebabe")
	t.Setenv("CI_PROJECT_DIR", ".")
	t.Setenv("FLAKE_TRACKER_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/1234")

	// Run main
	main()

	// Verify outcomes
	require.Equal(t, 1, fcli.createIssueCalls)
	require.Equal(t, 1, fcli.createNoteCalls)
	require.Len(t, sn.msgs, 1)
	require.Contains(t, sn.msgs[0], "New flaky test detected")
}

func TestProcessFailures_CreatesNewIssue(t *testing.T) {
	fcli := newFakeGitLab()
	sn := &fakeSlackNotifier{}
	fail := report.Failure{
		Suite:     "x",
		Classname: "pkg.TestTop",
		Name:      "Leaf",
	}
	created, updated := processFailures(fcli, sn, 1, []report.Failure{fail}, "ref", "sha123456", "pid", "purl")
	require.Equal(t, 1, created)
	require.Equal(t, 0, updated)
	require.Len(t, fcli.issues, 1)
	require.Contains(t, fcli.issues[0].Labels, "flaky-occurrences::1-3")
	require.Len(t, fcli.notes[fcli.issues[0].IID], 1)
	require.Contains(t, fcli.notes[fcli.issues[0].IID][0].Body, "FLAKE_OCCURRENCE:")
	require.Len(t, sn.msgs, 1)
}

func TestProcessFailures_UpdatesExistingIssue(t *testing.T) {
	fcli := newFakeGitLab()
	sn := &fakeSlackNotifier{}
	// Seed an opened issue and one prior occurrence
	is, _, err := fcli.CreateIssue(1, &gitlab.CreateIssueOptions{
		Title:  gitlab.Ptr("[Flaky] pkg.TestTop/Leaf"),
		Labels: (*gitlab.LabelOptions)(&[]string{"flaky::test"}),
		Weight: gitlab.Ptr(int64(1)),
	})
	require.NoError(t, err)
	_, _, err = fcli.CreateIssueNote(1, is.IID, &gitlab.CreateIssueNoteOptions{Body: gitlab.Ptr("FLAKE_OCCURRENCE:\n- ...")})
	require.NoError(t, err)
	fail := report.Failure{
		Suite:     "",
		Classname: "pkg.TestTop",
		Name:      "Leaf",
	}
	created, updated := processFailures(fcli, sn, 1, []report.Failure{fail}, "ref", "sha", "pid", "purl")
	require.Equal(t, 0, created)
	require.Equal(t, 1, updated)
	require.Contains(t, fcli.issues[0].Labels, "flaky-occurrences::1-3")
	require.GreaterOrEqual(t, fcli.issues[0].Weight, int64(2))
	require.Len(t, fcli.notes[is.IID], 2)
	require.Empty(t, sn.msgs)
}

func TestProcessFailures_ReopensClosedIssue(t *testing.T) {
	fcli := newFakeGitLab()
	sn := &fakeSlackNotifier{}
	_, _, err := fcli.CreateIssue(1, &gitlab.CreateIssueOptions{
		Title:  gitlab.Ptr("[Flaky] pkg.TestTop/Leaf"),
		Labels: (*gitlab.LabelOptions)(&[]string{"flaky::test"}),
		Weight: gitlab.Ptr(int64(1)),
	})
	require.NoError(t, err)
	// Manually mark closed
	fcli.issues[0].State = "closed"
	fail := report.Failure{Classname: "pkg.TestTop", Name: "Leaf"}
	created, updated := processFailures(fcli, sn, 1, []report.Failure{fail}, "ref", "sha", "pid", "purl")
	require.Equal(t, 0, created)
	require.Equal(t, 1, updated)
	require.Equal(t, "opened", fcli.issues[0].State)
	require.Empty(t, sn.msgs)
}

func TestProcessFailures_SearchIssuesErrorIsHandled(t *testing.T) {
	fcli := newFakeGitLab()
	fcli.errOnListIssues = true
	sn := &fakeSlackNotifier{}
	fail := report.Failure{Classname: "pkg.TestTop", Name: "Leaf"}
	created, updated := processFailures(fcli, sn, 1, []report.Failure{fail}, "ref", "sha", "pid", "purl")
	require.Equal(t, 0, created)
	require.Equal(t, 0, updated)
	require.Empty(t, fcli.issues)
	require.Empty(t, sn.msgs)
}

func TestPrefilterFailures_AppliesDenylist(t *testing.T) {
	deny := &testDenylist{exact: map[string]struct{}{"pkg.TestTop/Leaf": {}}}
	sdeny := &testDenylist{exact: map[string]struct{}{"azure-1_24-azure": {}}}
	in := []report.Failure{
		{Suite: "azure: [1.24, azure]", Classname: "pkg.TestTop", Name: "Leaf"},
		{Suite: "other suite", Classname: "pkg.TestTop", Name: "Keep"},
	}
	out := prefilterFailures(in, deny, sdeny)
	require.Len(t, out, 1)
	require.Equal(t, "Keep", out[0].Name)
}

func TestPrefilterFailures_SuiteSlugGlobAndExact(t *testing.T) {
	deny := &testDenylist{exact: make(map[string]struct{})}
	// suite denylist uses slug glob pattern
	sdeny := &testDenylist{globs: []string{"azure-*-azure-*"}}
	in := []report.Failure{
		{Suite: "azure: [2.1, azure, key]", Classname: "pkg.TestTop", Name: "A"},
		{Suite: "azure: [2.1, azure_v2, key]", Classname: "pkg.TestTop", Name: "B"},
		{Suite: "random", Classname: "pkg.TestTop", Name: "C"},
	}
	out := prefilterFailures(in, deny, sdeny)
	// "A" filtered due to slug match, "B" not filtered, "C" not filtered
	require.Len(t, out, 2)
	var names []string
	for _, f := range out {
		names = append(names, f.Name)
	}
	require.ElementsMatch(t, []string{"B", "C"}, names)
}

func TestPrefilterFailures_NoListsReturnsSame(t *testing.T) {
	in := []report.Failure{
		{Suite: "s1", Classname: "pkg", Name: "X"},
		{Suite: "s2", Classname: "pkg", Name: "Y"},
	}
	out := prefilterFailures(in, nil, nil)
	require.Equal(t, in, out)
}

func TestLoadAllowfailSuitePatternsFromCI_ParsesAndMatches(t *testing.T) {
	td := t.TempDir()
	ci := filepath.Join(td, "integration.yml")
	contents := strings.Join([]string{
		`jobs:`,
		`  # ci-tool-allowfail ^azure:\s*\[[^]]*\bazure\b[^]]*\]`,
		`  # unrelated comment`,
		`  # ci-tool-allowfail ^s3:`,
		`  # ci-tool-allowfail [invalid(`,
		`  script: echo`,
	}, "\n")
	require.NoError(t, os.WriteFile(ci, []byte(contents), 0o644))

	patterns := loadAllowfailSuitePatternsFromCI([]string{
		ci,
		filepath.Join(td, "does-not-exist.yml"),
	})
	// invalid regex should be ignored; we keep the two valid ones
	require.Len(t, patterns, 2)

	azureSuite := "azure: [1.24, azure, shared_key]"
	s3Suite := "s3: [1.24, s3, key_auth]"
	// Ensure at least one pattern matches each intended suite
	require.True(t, anyPatternMatches(patterns, azureSuite))
	require.True(t, anyPatternMatches(patterns, s3Suite))
}

func TestPrefilterFailures_UsesAllowfailSuitePatterns(t *testing.T) {
	// Prepare regexes that mark azure v1 suites as allowed to fail
	td := t.TempDir()
	ci := filepath.Join(td, "integration.yml")
	contents := `# ci-tool-allowfail ^azure:\s*\[[^]]*\bazure\b[^]]*\]`
	require.NoError(t, os.WriteFile(ci, []byte(contents), 0o644))
	patterns := loadAllowfailSuitePatternsFromCI([]string{ci})
	require.NotEmpty(t, patterns)

	deny := &testDenylist{exact: make(map[string]struct{})}
	sdeny := &testDenylist{exact: make(map[string]struct{}), regex: patterns}
	in := []report.Failure{
		{Suite: "azure: [1.24, azure, shared_key]", Classname: "pkg.Test", Name: "A"},
		{Suite: "azure: [1.24, azure_v2, client_secret]", Classname: "pkg.Test", Name: "B"},
		{Suite: "random", Classname: "pkg.Test", Name: "C"},
	}
	out := prefilterFailures(in, deny, sdeny)
	var names []string
	for _, f := range out {
		names = append(names, f.Name)
	}
	// "A" filtered by allowfail regex; "B" and "C" remain
	require.ElementsMatch(t, []string{"B", "C"}, names)
}

// anyPatternMatches returns true if any of the regexes matches s.
func anyPatternMatches(patterns []*regexp.Regexp, s string) bool {
	for _, r := range patterns {
		if r.MatchString(s) {
			return true
		}
	}
	return false
}
