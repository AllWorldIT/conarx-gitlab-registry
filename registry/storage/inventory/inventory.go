// Package inventory provides tools to conduct broad surveys of the contents
// of a registry.
package inventory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/reference"
)

// Inventory is a list of repository data, it provides a mutex enabling
// thread-safe access.
type Inventory struct {
	sync.Mutex
	Repositories []Repository `json:"repositories,omitempty"`
}

// Summary provides additional data which is derived from a completed inventory.
func (iv *Inventory) Summary() *Summary {
	s := &Summary{
		make([]Group, 0),
	}
	groupTotals := make(map[string]Group)

	for _, r := range iv.Repositories {
		gt, ok := groupTotals[r.Group]
		if !ok {
			groupTotals[r.Group] = Group{
				Name:            r.Group,
				TagCount:        r.TagCount,
				RepositoryCount: 1,
			}
		} else {
			gt.TagCount += r.TagCount
			gt.RepositoryCount++

			groupTotals[r.Group] = gt
		}
	}

	for _, gt := range groupTotals {
		s.Groups = append(s.Groups, gt)
	}

	return s
}

// Repository contains data gathered from a single repository.
type Repository struct {
	Group    string `json:"group" csv:"group"`
	Path     string `json:"path" csv:"path"`
	TagCount int    `json:"tagCount" csv:"tagCount"`
}

// Summary contains data which is calculated from a completed inventory.
type Summary struct {
	Groups []Group
}

// Group contains repository and tag totals across all repositories within the group.
type Group struct {
	Name            string
	TagCount        int
	RepositoryCount int
}

// Taker conducts an inventory of the embedded registry.
type Taker struct {
	registry distribution.Namespace

	// include tag counts for repositories
	tagTotals bool
}

// NewTaker is the constructor function for Taker.
func NewTaker(registry distribution.Namespace, tagTotals bool) *Taker {
	return &Taker{registry, tagTotals}
}

// Run traverses all repositories, gathers their data and returns an
// inventory of repositories encountered.
func (t *Taker) Run(ctx context.Context) (*Inventory, error) {
	repositoryEnumerator, ok := t.registry.(distribution.RepositoryEnumerator)
	if !ok {
		return nil, fmt.Errorf("unable to convert Namespace to RepositoryEnumerator")
	}

	start := time.Now()
	l := log.GetLogger(log.WithContext(ctx))
	l.Info("starting inventory")

	iv := &Inventory{
		sync.Mutex{},
		make([]Repository, 0),
	}

	var index int32
	err := repositoryEnumerator.Enumerate(ctx, func(repoName string) error {
		atomic.AddInt32(&index, 1)
		l = l.WithFields(log.Fields{"path": repoName, "count": index})
		l.Debug("inventoring repository start")
		start := time.Now()

		r := Repository{
			Path:  repoName,
			Group: strings.Split(repoName, "/")[0],
		}

		if t.tagTotals {
			named, err := reference.WithName(repoName)
			if err != nil {
				return fmt.Errorf("parsing repo name %s: %w", repoName, err)
			}
			repository, err := t.registry.Repository(ctx, named)
			if err != nil {
				return fmt.Errorf("constructing repository: %w", err)
			}

			tagService := repository.Tags(ctx)

			tags, err := tagService.All(ctx)
			if err != nil {
				if errors.As(err, &distribution.ErrRepositoryUnknown{}) {
					// No tags, but this repository would still be present in v2/_catalog
					// since Enumerate checks for the _layers directory. Rather than error
					// out or omit the repository, we'll return zero tags.
					tags = []string{}
				} else {
					return fmt.Errorf("listing tags: %w", err)
				}
			}

			r.TagCount = len(tags)
		}

		l = l.WithFields(log.Fields{
			"group":      r.Group,
			"tag_count":  r.TagCount,
			"duration_s": time.Since(start).Seconds(),
		})
		l.Info("inventoring repository complete")

		iv.Lock()
		defer iv.Unlock()

		iv.Repositories = append(iv.Repositories, r)

		return nil
	})
	if err != nil {
		return nil, err
	}

	l.WithFields(log.Fields{"duration_s": time.Since(start).Seconds()}).Info("inventory complete")

	return iv, nil
}
