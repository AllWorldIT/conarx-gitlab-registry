package report

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	gitlab "gitlab.com/gitlab-org/api/client-go"
)

func TestFailure_SnippetAndContext(t *testing.T) {
	f1 := Failure{
		SystemOutput: "  some output\n",
		StackTrace:   "  some stack\n",
	}
	require.Equal(t, "some output", f1.Snippet())
	require.Equal(t, "some stack", f1.Context())

	f2 := Failure{
		SystemOutput: "   ",
		StackTrace:   "trace only",
	}
	require.Equal(t, "trace only", f2.Snippet())
	require.Equal(t, "trace only", f2.Context())
}

func TestFailure_SnippetTruncated(t *testing.T) {
	long := "line1\nline2\nline3\n"
	f := Failure{SystemOutput: long}
	// Choose a max that cuts inside "line2", so it prefers newline before max
	got := f.SnippetTruncated(8) // "line1\nli" => should cut at newline after "line1"
	require.True(t, strings.HasPrefix(got, "line1\n"), "expected prefix 'line1\\n', got %q", got)
	require.Contains(t, got, "... (truncated)")

	// No newline case: single long line
	oneLine := strings.Repeat("a", 20)
	f2 := Failure{SystemOutput: oneLine}
	got2 := f2.SnippetTruncated(10)
	require.True(t, strings.HasPrefix(got2, strings.Repeat("a", 10)), "expected 10 leading 'a', got %q", got2)
	require.Contains(t, got2, "... (truncated)")
}

func TestFailure_ExtractTestName(t *testing.T) {
	f := Failure{
		SystemOutput: strings.Join([]string{
			"=== RUN   TestRepo/Op/Deep",
			"--- PASS: TestRepo/Op (0.00s)",
			"=== RUN   TestRepo/Op/LessDeep",
			"--- FAIL: TestRepo/Op/Deep/Leaf (0.00s)",
		}, "\n"),
	}
	require.Equal(t, "TestRepo/Op/Deep/Leaf", f.ExtractTestName())

	// Fallback to RUN lines if no FAIL present
	f2 := Failure{
		SystemOutput: strings.Join([]string{
			"=== RUN   TestA/B/C",
			"=== RUN   TestA/B",
		}, "\n"),
	}
	require.Equal(t, "TestA/B/C", f2.ExtractTestName())
}

func TestFailure_NormalizedID(t *testing.T) {
	tests := []struct {
		name      string
		classname string
		testName  string
		snippet   string
		expected  string
	}{
		{
			name:      "uses snippet-most-specific with package",
			classname: "gitlab.com/group/proj/pkg.TestRepoOp",
			testName:  "Leaf",
			snippet: strings.Join([]string{
				"=== RUN   TestRepoOp/Sub/Leaf2",
				"--- FAIL: TestRepoOp/Sub/Leaf2 (0.01s)",
			}, "\n"),
			expected: "gitlab.com/group/proj/pkg.TestRepoOp/Sub/Leaf2",
		},
		{
			name:      "fallback to classname parent + name when snippet empty",
			classname: "gitlab.com/group/proj/pkg.TestRepo",
			testName:  "Sub/Leaf",
			snippet:   "",
			expected:  "gitlab.com/group/proj/pkg.TestRepo/Sub/Leaf",
		},
		{
			name:      "package + top-level test",
			classname: "gitlab.com/group/proj/pkg",
			testName:  "TestTop",
			snippet:   "",
			expected:  "gitlab.com/group/proj/pkg.TestTop",
		},
		{
			name:      "no package, parent in classname-like string",
			classname: "TestTop",
			testName:  "Leaf",
			snippet:   "",
			expected:  "TestTop/Leaf",
		},
		{
			name:      "no package and no parent",
			classname: "",
			testName:  "TestAlone",
			snippet:   "",
			expected:  "TestAlone",
		},
		{
			name:      "parent from classname is prepended to top-level child test",
			classname: "gitlab.com/group/proj/pkg.TestAzureDriverSuite",
			testName:  "TestMaxUploadSize",
			snippet:   "",
			expected:  "gitlab.com/group/proj/pkg.TestAzureDriverSuite/TestMaxUploadSize",
		},
		{
			name:      "does not double-prepend when name already includes parent",
			classname: "gitlab.com/group/proj/pkg.TestTop",
			testName:  "TestTop/Sub",
			snippet:   "",
			expected:  "gitlab.com/group/proj/pkg.TestTop/Sub",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := Failure{
				Classname:    tc.classname,
				Name:         tc.testName,
				SystemOutput: tc.snippet,
			}
			require.Equal(t, tc.expected, f.NormalizedID())
		})
	}
}

func TestFailuresFromSDK(t *testing.T) {
	tr := &gitlab.PipelineTestReport{
		TestSuites: []*gitlab.PipelineTestSuites{
			{
				Name: "suite1",
				TestCases: []*gitlab.PipelineTestCases{
					{
						Name:         "Leaf",
						Classname:    "gitlab.com/group/proj/pkg.TestRepoOp",
						File:         "x_test.go",
						Status:       "failed",
						SystemOutput: "=== RUN   TestRepoOp/Sub/Leaf\n--- FAIL: TestRepoOp/Sub/Leaf (0.01s)\n",
						StackTrace:   "stack line",
					},
					{
						Name:         "Other",
						Classname:    "gitlab.com/group/proj/pkg",
						File:         "y_test.go",
						Status:       "passed",
						SystemOutput: "not included",
						StackTrace:   "",
					},
					{
						Name:         "ErrLeaf",
						Classname:    "gitlab.com/group/proj/pkg.TestRepo",
						File:         "z_test.go",
						Status:       "error",
						SystemOutput: map[string]any{"stdout": "line1", "stderr": "line2"},
						StackTrace:   "trace here",
					},
				},
			},
		},
	}
	failures := failuresFromSDK(tr)
	require.Len(t, failures, 2)
	f0 := failures[0]
	require.Equal(t, "Leaf", f0.Name)
	require.Equal(t, "gitlab.com/group/proj/pkg.TestRepoOp", f0.Classname)
	require.Equal(t, "x_test.go", f0.File)
	require.Contains(t, f0.SystemOutput, "FAIL: TestRepoOp/Sub/Leaf")
	require.Equal(t, "stack line", f0.Context())
	require.Equal(t, "suite1::gitlab.com/group/proj/pkg.TestRepoOp/Sub/Leaf", f0.NormalizedID())

	f1 := failures[1]
	require.Equal(t, "ErrLeaf", f1.Name)
	require.Equal(t, "gitlab.com/group/proj/pkg.TestRepo", f1.Classname)
	require.Equal(t, "z_test.go", f1.File)
	require.Equal(t, "line1\nline2", f1.SystemOutput)
	require.Equal(t, "trace here", f1.Context())
}

type fakePipelinesClient struct {
	report *gitlab.PipelineTestReport
	err    error
}

func (f *fakePipelinesClient) GetPipelineTestReport(_, _ int64, _ ...gitlab.RequestOptionFunc) (*gitlab.PipelineTestReport, *gitlab.Response, error) {
	return f.report, &gitlab.Response{}, f.err
}

func TestFetchFailures_UsesClient(t *testing.T) {
	tr := &gitlab.PipelineTestReport{
		TestSuites: []*gitlab.PipelineTestSuites{
			{
				Name: "s",
				TestCases: []*gitlab.PipelineTestCases{
					{Name: "A", Classname: "pkg.Test", File: "a_test.go", Status: "failed", SystemOutput: "x", StackTrace: ""},
				},
			},
		},
	}
	cli := &fakePipelinesClient{report: tr}
	got, err := FetchFailures(cli, 1, 2)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "A", got[0].Name)

	cli2 := &fakePipelinesClient{report: nil, err: fmt.Errorf("boom")}
	_, err = FetchFailures(cli2, 1, 2)
	require.Error(t, err)
}

func TestNormalizedID_SuiteSlugging(t *testing.T) {
	f := Failure{
		Suite:     "azure: [1.24, azure, shared key]",
		Classname: "pkg.TestAzureDriverSuite",
		Name:      "TestMaxUploadSize",
		// no snippet => fallback path pkg + parent/name
	}
	got := f.NormalizedID()
	wantPrefix := "azure-1_24-azure-shared-key::"
	require.True(t, strings.HasPrefix(got, wantPrefix), "got %q, expected prefix %q", got, wantPrefix)
	require.Equal(t, wantPrefix+"pkg.TestAzureDriverSuite/TestMaxUploadSize", got)
}

func TestSuiteSlug_VariousInputs(t *testing.T) {
	cases := map[string]string{
		"":                                   "",
		"  ":                                 "",
		"AZURE: [1.24, AZURE, Shared_Key]":   "azure-1_24-azure-shared_key",
		"azure(1.24) azure.shared_key":       "azure-1_24-azure_shared_key",
		"azure - 1.24 / azure \\ shared_key": "azure-1_24-azure-shared_key",
		"...azure___1.24___azure...":         "azure_1_24_azure",
	}
	for in, want := range cases {
		got := SuiteSlug(in)
		require.Equal(t, want, got, "in=%q", in)
	}
}

func TestExtractTextField_Cases(t *testing.T) {
	// plain string
	f1 := Failure{SystemOutput: " hello "}
	require.Equal(t, "hello", f1.Snippet())

	// whitespace SystemOutput falls back to StackTrace
	f4 := Failure{
		SystemOutput: "   ",
		StackTrace:   "trace only",
	}
	require.Equal(t, "trace only", f4.Snippet())
	// empty
	f5 := Failure{}
	require.Empty(t, f5.Snippet())
}

func TestUniqueLeafFailures_KeepsDifferentSuites(t *testing.T) {
	// Same underlying test but different suites => different NormalizedID (suite prefix),
	// both should be kept as leaves.
	a := Failure{
		Suite:        "azure: [1.24, azure, shared key]",
		Classname:    "pkg.TestTop",
		Name:         "Leaf",
		SystemOutput: "",
	}
	b := Failure{
		Suite:        "azure: [1.25, azure, shared key]",
		Classname:    "pkg.TestTop",
		Name:         "Leaf",
		SystemOutput: "",
	}
	out := UniqueLeafFailures([]Failure{a, b})
	require.Len(t, out, 2)
	require.NotEqual(t, out[0].NormalizedID(), out[1].NormalizedID())
}

func TestUniqueLeafFailures_DeduplicatesIdenticalLeaf(t *testing.T) {
	// Both entries normalize to the same deepest subtest path; keep only one.
	f1 := Failure{
		Name:      "TestAzureDriverSuite",
		Classname: "gitlab.com/group/proj/pkg.TestAzureDriverSuite",
		SystemOutput: strings.Join([]string{
			"=== RUN   TestAzureDriverSuite/TestMaxUploadSize",
			"--- FAIL: TestAzureDriverSuite/TestMaxUploadSize (1.00s)",
		}, "\n"),
	}
	f2 := Failure{
		Name:      "TestMaxUploadSize",
		Classname: "gitlab.com/group/proj/pkg.TestAzureDriverSuite",
		SystemOutput: strings.Join([]string{
			"--- FAIL: TestAzureDriverSuite/TestMaxUploadSize (1.00s)",
		}, "\n"),
	}
	out := UniqueLeafFailures([]Failure{f1, f2})
	require.Len(t, out, 1)
	want := "gitlab.com/group/proj/pkg.TestAzureDriverSuite/TestMaxUploadSize"
	require.Equal(t, want, out[0].NormalizedID())
}

func TestUniqueLeafFailures_DropsParentWhenChildExists(t *testing.T) {
	parent := Failure{
		Name:      "TestTop",
		Classname: "gitlab.com/group/proj/pkg",
		SystemOutput: strings.Join([]string{
			"=== RUN   TestTop",
			"--- FAIL: TestTop (0.10s)",
		}, "\n"),
	}
	child := Failure{
		Name:      "Leaf",
		Classname: "gitlab.com/group/proj/pkg.TestTop",
		SystemOutput: strings.Join([]string{
			"=== RUN   TestTop/Leaf",
			"--- FAIL: TestTop/Leaf (0.05s)",
		}, "\n"),
	}
	out := UniqueLeafFailures([]Failure{parent, child})
	require.Len(t, out, 1)
	want := "gitlab.com/group/proj/pkg.TestTop/Leaf"
	require.Equal(t, want, out[0].NormalizedID())
}
