package report

import (
	"regexp"
	"sort"
	"strings"

	gitlab "gitlab.com/gitlab-org/api/client-go"
)

// Failure is a minimal representation of a failing test case needed by the tracker.
type Failure struct {
	Suite        string
	Name         string
	Classname    string
	File         string
	SystemOutput string
	StackTrace   string
}

// PipelinesClient abstracts the pipelines API used to fetch test reports.
type PipelinesClient interface {
	GetPipelineTestReport(projectID, pipelineID int64, opts ...gitlab.RequestOptionFunc) (*gitlab.PipelineTestReport, *gitlab.Response, error)
}

func FetchFailures(cli PipelinesClient, projectID, pipelineID int64) ([]Failure, error) {
	tr, _, err := cli.GetPipelineTestReport(projectID, pipelineID)
	if err != nil {
		return nil, err
	}
	return failuresFromSDK(tr), nil
}

// extractSystemOutput converts an SDK-provided system_output (string or object) into text.
func extractSystemOutput(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case map[string]any:
		var parts []string
		if s, ok := t["stdout"].(string); ok && strings.TrimSpace(s) != "" {
			parts = append(parts, s)
		}
		if s, ok := t["stderr"].(string); ok && strings.TrimSpace(s) != "" {
			parts = append(parts, s)
		}
		if len(parts) > 0 {
			return strings.TrimSpace(strings.Join(parts, "\n"))
		}
		for _, k := range []string{"value", "message", "content", "trace", "error", "text"} {
			if s, ok := t[k].(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

// failuresFromSDK converts a PipelineTestReport into []Failure applying the same filtering rules.
func failuresFromSDK(tr *gitlab.PipelineTestReport) []Failure {
	var failures []Failure
	if tr == nil {
		return failures
	}
	for _, s := range tr.TestSuites {
		if s == nil {
			continue
		}
		for _, c := range s.TestCases {
			if c == nil {
				continue
			}
			st := strings.ToLower(strings.TrimSpace(c.Status))
			if st != "failed" && st != "error" {
				continue
			}
			systemOutput := strings.TrimSpace(extractSystemOutput(c.SystemOutput))
			stackTrace := strings.TrimSpace(c.StackTrace)
			failures = append(failures, Failure{
				Suite:        s.Name,
				Name:         c.Name,
				Classname:    c.Classname,
				File:         c.File,
				SystemOutput: systemOutput,
				StackTrace:   stackTrace,
			})
		}
	}
	return failures
}

// Snippet returns the preferred human-readable failure details:
// system output if available, otherwise the stack trace.
func (f Failure) Snippet() string {
	if s := strings.TrimSpace(f.SystemOutput); s != "" {
		return s
	}
	return strings.TrimSpace(f.StackTrace)
}

// Context returns the full stack trace or additional error context.
func (f Failure) Context() string {
	return strings.TrimSpace(f.StackTrace)
}

// SnippetTruncated returns the snippet limited to maxChar characters,
// attempting to preserve whole lines when possible.
func (f Failure) SnippetTruncated(maxChar int) string {
	return trimStringPreservingLines(f.Snippet(), maxChar)
}

// ExtractTestName scans the snippet and context for the most specific Go test path,
// e.g., "TestParent/Sub/Leaf". It prefers FAIL lines, then RUN lines, and chooses
// the candidate with the most "/" segments, then the longest length.
func (f Failure) ExtractTestName() string {
	sources := []string{f.Snippet(), f.Context()}
	candidates := make([]string, 0, 8)
	for _, src := range sources {
		if src == "" {
			continue
		}
		for _, m := range reFailLine.FindAllStringSubmatch(src, -1) {
			if len(m) > 1 && strings.HasPrefix(m[1], "Test") {
				candidates = append(candidates, strings.TrimSpace(m[1]))
			}
		}
		for _, m := range reRunLine.FindAllStringSubmatch(src, -1) {
			if len(m) > 1 && strings.HasPrefix(m[1], "Test") {
				candidates = append(candidates, strings.TrimSpace(m[1]))
			}
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	best := candidates[0]
	bestSlash := strings.Count(best, "/")
	for _, c := range candidates[1:] {
		if sc := strings.Count(c, "/"); sc > bestSlash || (sc == bestSlash && len(c) > len(best)) {
			best = c
			bestSlash = sc
		}
	}
	return best
}

// NormalizedID returns a stable identifier for this failure's test case.
// It ensures Go subtests are uniquely referenced by including the parent test.
// The resulting format aims to be: "<package>.<ParentTest[/Subtest[/...]]>".
func (f Failure) NormalizedID() string {
	classname := strings.TrimSpace(f.Classname)
	name := strings.TrimSpace(f.Name)

	var base string
	// Prefer extracting the most specific path from snippet/context if possible
	if sn := strings.TrimSpace(f.ExtractTestName()); sn != "" {
		pkg, _ := extractPackageAndParent(classname)
		if pkg != "" {
			base = pkg + "." + sn
		} else {
			base = sn
		}
	} else {
		// Fallback to classname/name inference
		pkg, parent := extractPackageAndParent(classname)
		fullName := name
		// Safely prepend parent when appropriate:
		// - if name is empty, use parent
		// - if name isn't already prefixed with parent and isn't equal to parent, prefix it
		if parent != "" {
			if strings.TrimSpace(fullName) == "" {
				fullName = parent
			}
			if strings.TrimSpace(fullName) != "" && !strings.HasPrefix(fullName, parent+"/") && fullName != parent {
				fullName = parent + "/" + fullName
			}
		}
		if pkg != "" {
			base = pkg + "." + fullName
		} else {
			base = fullName
		}
	}
	// Prefix with suite for additional uniqueness if available
	if s := normalizeSuiteName(f.Suite); s != "" {
		return s + "::" + base
	}
	return base
}

// normalizeSuiteName converts a test suite/job name into a compact, stable slug:
// - lowercases
// - replaces any non-alphanumeric with single '-'
// - trims leading/trailing '-'
func normalizeSuiteName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToLower(s)
	var b strings.Builder
	prevHyphen := false
	prevUnderscore := false
	for _, r := range s {
		// alphanumeric characters are passed through
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			_, _ = b.WriteRune(r)
			prevHyphen = false
			prevUnderscore = false
			continue
		}
		// dots and underscores become underscores (collapse multiple) and are passed through
		if r == '.' || r == '_' {
			if !prevUnderscore {
				_ = b.WriteByte('_')
				prevUnderscore = true
			}
			prevHyphen = false
			continue
		}
		// collapse any punctuation/whitespace into a single hyphen
		if !prevHyphen {
			_ = b.WriteByte('-')
			prevHyphen = true
		}
		prevUnderscore = false
	}
	out := b.String()
	out = strings.Trim(out, "-_")
	return out
}

// SuiteSlug returns the normalized/slugged form of a suite name suitable for IDs and matching.
func SuiteSlug(s string) string {
	return normalizeSuiteName(s)
}

// extractPackageAndParent analyzes a test classname and returns:
// - pkg: a best-effort package-like prefix (often the Go package path)
// - parent: a top-level test name if present (e.g., "TestParent")
func extractPackageAndParent(classname string) (pkg, parent string) {
	cn := strings.TrimSpace(classname)
	if cn == "" {
		return "", ""
	}
	// Many JUnit serializers use "<package>" or "<package>.<ParentTest>" as classname.
	if i := strings.LastIndex(cn, "."); i >= 0 {
		p, tail := cn[:i], cn[i+1:]
		if strings.HasPrefix(tail, "Test") {
			return p, tail
		}
		// When tail is not a test name, treat the whole classname as package.
		return cn, ""
	}
	// No dot in classname: if it looks like a test, treat as parent; otherwise, as package.
	if strings.HasPrefix(cn, "Test") {
		return "", cn
	}
	return cn, ""
}

// trimStringPreservingLines returns at most max characters from s, preserving
// whole lines when possible. If truncated, it appends an ellipsis marker.
func trimStringPreservingLines(s string, maxChar int) string {
	if len(s) <= maxChar {
		return s
	}
	// Try to cut at a newline boundary before max
	cutoff := maxChar
	if idx := strings.LastIndex(s[:maxChar], "\n"); idx > 0 {
		cutoff = idx
	}
	return s[:cutoff] + "\n... (truncated)"
}

var (
	// Matches lines like: "--- FAIL: TestParent/Sub (0.00s)"
	reFailLine = regexp.MustCompile(`(?m)^--- FAIL:\s+([^\s(]+)`)
	// Matches lines like: "=== RUN   TestParent/Sub"
	reRunLine = regexp.MustCompile(`(?m)^=== RUN\s+([^\s]+)`)
)

// UniqueLeafFailures keeps only unique failures by their normalized id and
// removes parent tests when a more specific subtest exists.
func UniqueLeafFailures(in []Failure) []Failure {
	// Collect normalized ids (skip empty)
	type item struct {
		id  string
		idx int
	}
	items := make([]item, 0, len(in))
	for i, f := range in {
		id := strings.TrimSpace(f.NormalizedID())
		if id == "" {
			continue
		}
		items = append(items, item{id: id, idx: i})
	}
	if len(items) == 0 {
		return nil
	}
	// Build a set of all ids for quick lookup
	all := make(map[string]struct{}, len(items))
	for _, it := range items {
		all[it.id] = struct{}{}
	}
	// Determine which ids are leaves (no other id has "id + /" prefix)
	isLeaf := make(map[string]bool, len(items))
	for _, it := range items {
		prefix := it.id + "/"
		leaf := true
		for other := range all {
			if other != it.id && strings.HasPrefix(other, prefix) {
				leaf = false
				break
			}
		}
		isLeaf[it.id] = leaf
	}
	// Keep only leaves and de-duplicate identical ids (first occurrence wins)
	seen := make(map[string]struct{}, len(items))
	out := make([]Failure, 0, len(items))
	// Preserve a stable order: sort by first-seen index
	sort.SliceStable(items, func(i, j int) bool { return items[i].idx < items[j].idx })
	for _, it := range items {
		if !isLeaf[it.id] {
			continue
		}
		if _, ok := seen[it.id]; ok {
			continue
		}
		seen[it.id] = struct{}{}
		out = append(out, in[it.idx])
	}
	return out
}
