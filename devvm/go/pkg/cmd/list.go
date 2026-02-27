package cmd

import (
	"context"
	"fmt"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/misc"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/provisioning"
)

func getVMInfoString(instance *computepb.Instance) string {
	var uptime string
	startTime, err := time.Parse("2006-01-02T15:04:05.999-07:00", instance.GetLastStartTimestamp())
	if err != nil {
		uptime = fmt.Sprintf("invalid start timestamp: %s", err)
	} else {
		uptime = time.Since(startTime).String()
	}

	ip, err := provisioning.ExtractPublicIPFromVMInfo(instance)
	if err != nil {
		return fmt.Sprintf("%s %s <%s> %s", instance.GetName(), instance.GetStatus(), err.Error(), uptime)
	}

	return fmt.Sprintf("%s %s %s %s", instance.GetName(), instance.GetStatus(), ip, uptime)
}

func ListVMs(ctx context.Context, zone string) error {
	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create instance client: %w", err)
	}
	defer instancesClient.Close()

	instances, err := misc.ListDevVMs(ctx, instancesClient, zone)
	if err != nil {
		return err
	}

	for _, instance := range instances {
		infoString := getVMInfoString(instance)
		zoneName := misc.ExtractZoneName(instance.GetZone())
		fmt.Printf("%s\t- %s\n", zoneName, infoString)
	}

	return nil
}
