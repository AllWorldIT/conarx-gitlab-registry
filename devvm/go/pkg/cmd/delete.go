package cmd

import (
	"context"
	"fmt"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/misc" // Import added
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/provisioning"
)

func DeleteVM(ctx context.Context, zone string, instancesNames []string) error {
	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create instance client: %w", err)
	}
	defer instancesClient.Close()

	for _, instanceName := range instancesNames {
		// Resolve the zone using the helper.
		// If zone is provided, it returns it.
		// If not, it looks up the instance.
		// If ambiguous (multiple instances), it returns an error.
		targetZone, err := misc.ResolveInstanceZone(ctx, instancesClient, instanceName, zone)
		if err != nil {
			return fmt.Errorf("resolving zone: %w", err)
		}

		op, err := instancesClient.Delete(
			ctx,
			&computepb.DeleteInstanceRequest{
				Project:  constants.GCEProject,
				Zone:     targetZone,
				Instance: instanceName,
			},
			provisioning.DefaultRetryOption(),
		)
		if err != nil {
			return fmt.Errorf("unable to delete VM %s: %w", instanceName, err)
		}
		// TODO(prozlach) - add a pretty spinner
		fmt.Printf("Deleting instance %s...", instanceName)
		if err = op.Wait(ctx); err != nil {
			fmt.Printf(" - FAILED\n")
			return fmt.Errorf("unable to wait for the operation: %w", err)
		}

		fmt.Printf(" - OK!\n")
	}

	return nil
}
