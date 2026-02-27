// nolint: revive // I like name `misc`
package misc

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb" // Ensure this is imported if needed for specific options, or standard iterator
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

// ListDevVMs retrieves all VM instances labeled as devvms.
// If zone is provided, it lists instances in that zone.
// If zone is empty, it aggregates instances from all zones.
func ListDevVMs(ctx context.Context, client *compute.InstancesClient, zone string) ([]*computepb.Instance, error) {
	var instances []*computepb.Instance
	filter := proto.String("labels.devvm:true")

	if zone != "" {
		// List in a specific zone
		req := &computepb.ListInstancesRequest{
			Project: constants.GCEProject,
			Zone:    zone,
			Filter:  filter,
		}
		it := client.List(ctx, req)
		for {
			resp, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to list instances in zone %s: %w", zone, err)
			}
			instances = append(instances, resp)
		}
	} else {
		// List across all zones
		req := &computepb.AggregatedListInstancesRequest{
			Project: constants.GCEProject,
			Filter:  filter,
		}
		it := client.AggregatedList(ctx, req)
		for {
			pair, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to list aggregated instances: %w", err)
			}
			if pair.Value.Instances != nil {
				instances = append(instances, pair.Value.Instances...)
			}
		}
	}

	return instances, nil
}

// ExtractZoneName helper to parse zone from URL if necessary
func ExtractZoneName(zoneURL string) string {
	// If it's already just a zone name (no slashes), return as-is
	if !strings.Contains(zoneURL, "/") {
		return zoneURL
	}

	u, err := url.Parse(zoneURL)
	if err != nil {
		// Fallback: just use path.Base on the raw string
		return path.Base(zoneURL)
	}
	return path.Base(u.Path)
}
