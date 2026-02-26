package provisioning

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/googleapis/gax-go/v2"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/proto"
)

// GetVMPublicIP fetches VM info and extracts public IP in one call
func GetVMPublicIP(
	ctx context.Context,
	instanceName, project, zone string,
	client *compute.InstancesClient,
) (string, error) {
	instanceInfo, notFound, err := GetVMInfo(ctx, instanceName, project, zone, client)
	if err != nil {
		if notFound {
			return "", fmt.Errorf("instance %s not found in zone %s", instanceName, zone)
		}
		return "", fmt.Errorf("unable to fetch information about instance %s: %w", instanceName, err)
	}

	publicIP, err := ExtractPublicIPFromVMInfo(instanceInfo)
	if err != nil {
		return "", fmt.Errorf("unable to determine public IP of instance %s: %w", instanceName, err)
	}

	return publicIP, nil
}

// DefaultRetryOption returns the standard retry policy for GCE operations
func DefaultRetryOption() gax.CallOption {
	return gax.WithRetry(
		func() gax.Retryer {
			return gax.OnErrorFunc(
				gax.Backoff{
					Initial:    1 * time.Second,
					Max:        32 * time.Second,
					Multiplier: 2,
				},
				// Wait ~1 minutes max in total
				ShouldRetryFunc(5, true),
			)
		},
	)
}

func GetGCERegion(zone string) (string, error) {
	idx := strings.LastIndex(zone, "-")
	if idx == -1 {
		return "", fmt.Errorf("malformed zone: %s", zone)
	}
	return zone[:idx], nil
}

func EnsureVM(
	ctx context.Context,
	instanceName, gceProject, zone, pubKey, region, registryBranch, gitLabBranch string,
	instancesClient *compute.InstancesClient,
	imageName, deploymentType string,
	eeVersion, noDeploy bool,
) (bool, error) {
	instanceInfo, notFound, err := GetVMInfo(ctx, instanceName, gceProject, zone, instancesClient)
	if err != nil {
		if !notFound {
			return false, fmt.Errorf("unable to fetch VM instance info: %w", err)
		}

		err = CreateVM(
			ctx,
			instanceName,
			pubKey,
			gceProject,
			zone,
			region,
			registryBranch, gitLabBranch,
			instancesClient,
			map[string]string{
				"source": "cmdline",
			},
			imageName,
			deploymentType,
			eeVersion,
			noDeploy,
		)
		if err != nil {
			return false, fmt.Errorf("unable to create VM: %w", err)
		}
		return true, nil
	}

	if *instanceInfo.Status != "RUNNING" {
		return false, fmt.Errorf("VM exists and is in state %s", *instanceInfo.Status)
	}

	return false, nil
}

func ExtractPublicIPFromVMInfo(instanceInfo *computepb.Instance) (string, error) {
	numNetIfs := len(instanceInfo.NetworkInterfaces)
	if numNetIfs != 1 {
		return "", fmt.Errorf("devvm has more than one network interface: (%d)", numNetIfs)
	}
	netIfInfo := instanceInfo.NetworkInterfaces[0]

	numAccessConfigs := len(netIfInfo.AccessConfigs)
	if numAccessConfigs == 0 {
		return "", fmt.Errorf("devvm does not have access to internet")
	}
	if numAccessConfigs > 1 {
		return "", fmt.Errorf("devvm has more than one Access Config defined (%d)", numAccessConfigs)
	}
	if netIfInfo.AccessConfigs[0].NatIP == nil {
		return "", fmt.Errorf("IP is undefined")
	}

	return *netIfInfo.AccessConfigs[0].NatIP, nil
}

func GetVMInfo(
	ctx context.Context,
	instanceName, project, zone string,
	instancesClient *compute.InstancesClient,
) (
	*computepb.Instance, bool, error,
) {
	instanceInfo, err := instancesClient.Get(
		ctx,
		&computepb.GetInstanceRequest{
			Instance: instanceName,
			Project:  project,
			Zone:     zone,
		},
		DefaultRetryOption(),
	)
	if err != nil {
		isNotFound, errParsed := isGCPNotFoundErr(err)
		return nil, isNotFound, errParsed
	}

	return instanceInfo, false, nil
}

func ShouldRetryFunc(maxRetries int, treat404AsFinal bool) func(err error) bool {
	totalRetries := 0
	return func(err error) bool {
		totalRetries++
		if totalRetries > maxRetries {
			_, _ = fmt.Fprintf(os.Stderr, "GCE API retry reached max retries. err: %v, max_retries: %d\n", err, maxRetries)
			return false
		}
		var gerr *googleapi.Error
		if ok := errors.As(err, &gerr); ok {
			if gerr.Code == http.StatusForbidden || treat404AsFinal && gerr.Code == http.StatusNotFound {
				var reason string
				if len(gerr.Errors) > 0 {
					reason = gerr.Errors[0].Reason
				}
				_, _ = fmt.Fprintf(
					os.Stderr,
					"will not retry non-retryable GCE API error %v, err_reason: %s, total_retries: %d, max_retries: %d\n",
					err, reason, totalRetries, maxRetries,
				)
				return false
			}
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			_, _ = fmt.Fprintf(
				os.Stderr,
				"will not retry non-retryable GCE API error %v, total_retries: %d, max_retries: %d\n",
				err, totalRetries, maxRetries,
			)
			return false
		}

		_, _ = fmt.Fprintf(
			os.Stderr,
			"will retry GCE API error %v, total_retries: %d, max_retries: %d\n",
			err, totalRetries, maxRetries,
		)
		return true
	}
}

func isGCPNotFoundErr(err error) (bool, error) {
	var gAPIError *googleapi.Error
	if errors.As(err, &gAPIError) {
		sc := gAPIError.Code
		switch {
		case sc >= http.StatusBadRequest && sc < http.StatusInternalServerError:
			// Client errors (BadRequest/Unauthorized etc) are Fatal. We
			// also return true to indicate we have NotFound which is a
			// special case in some context.
			return sc == http.StatusNotFound, fmt.Errorf("client error from gcp: %w", gAPIError)
		default:
			// Everything else is a normal error which can be
			// logged as a failure and then the reconciler will try
			// again on the next loop.
			return false, fmt.Errorf("error from gcp: %w", gAPIError)
		}
	}

	// Error should be a googleapi.Error
	return false, fmt.Errorf("unexpected error from gcp: %w", err)
}

// taken from https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type#go
func customMachineTypeURI(zone, cpuSeries string, coreCount, memory int) (string, error) {
	const (
		n1       = "custom"
		n2       = "n2-custom"
		n2d      = "n2d-custom"
		e2       = "e2-custom"
		e2Micro  = "e2-custom-micro"
		e2Small  = "e2-custom-small"
		e2Medium = "e2-custom-medium"
	)

	type typeLimit struct {
		allowedCores     []int
		minMemPerCore    int
		maxMemPerCore    int
		allowExtraMemory bool
		extraMemoryLimit int
	}

	makeRange := func(start, end, step int) []int {
		if step <= 0 || end < start {
			return nil
		}
		s := make([]int, 0, 1+(end-start)/step)
		for start <= end {
			s = append(s, start)
			start += step
		}
		return s
	}

	containsString := func(s []string, str string) bool {
		for _, v := range s {
			if v == str {
				return true
			}
		}

		return false
	}

	containsInt := func(nums []int, n int) bool {
		for _, v := range nums {
			if v == n {
				return true
			}
		}

		return false
	}

	var (
		cpuSeriesE2Limit = typeLimit{
			allowedCores:  makeRange(2, 33, 2),
			minMemPerCore: 512,
			maxMemPerCore: 8192,
		}
		cpuSeriesE2MicroLimit  = typeLimit{minMemPerCore: 1024, maxMemPerCore: 2048}
		cpuSeriesE2SmallLimit  = typeLimit{minMemPerCore: 2048, maxMemPerCore: 4096}
		cpuSeriesE2MediumLimit = typeLimit{minMemPerCore: 4096, maxMemPerCore: 8192}
		cpuSeriesN2Limit       = typeLimit{
			allowedCores:  append(makeRange(2, 33, 2), makeRange(36, 129, 4)...),
			minMemPerCore: 512, maxMemPerCore: 8192,
			allowExtraMemory: true,
			extraMemoryLimit: 624 << 10,
		}
		cpuSeriesN2DLimit = typeLimit{
			allowedCores:  []int{2, 4, 8, 16, 32, 48, 64, 80, 96},
			minMemPerCore: 512, maxMemPerCore: 8192,
			allowExtraMemory: true,
			extraMemoryLimit: 768 << 10,
		}
		cpuSeriesN1Limit = typeLimit{
			allowedCores:     append([]int{1}, makeRange(2, 97, 2)...),
			minMemPerCore:    922,
			maxMemPerCore:    6656,
			allowExtraMemory: true,
			extraMemoryLimit: 624 << 10,
		}
	)

	typeLimitsMap := map[string]typeLimit{
		n1:       cpuSeriesN1Limit,
		n2:       cpuSeriesN2Limit,
		n2d:      cpuSeriesN2DLimit,
		e2:       cpuSeriesE2Limit,
		e2Micro:  cpuSeriesE2MicroLimit,
		e2Small:  cpuSeriesE2SmallLimit,
		e2Medium: cpuSeriesE2MediumLimit,
	}

	if !containsString([]string{e2, n1, n2, n2d}, cpuSeries) {
		return "", fmt.Errorf("incorrect cpu type: %v", cpuSeries)
	}

	tl := typeLimitsMap[cpuSeries]

	// Check whether the requested parameters are allowed.
	// Find more information about limitations of custom machine types at:
	// https://cloud.google.com/compute/docs/general-purpose-machines#custom_machine_types

	// Check the number of cores
	if len(tl.allowedCores) > 0 && !containsInt(tl.allowedCores, coreCount) {
		return "", fmt.Errorf(
			"invalid number of cores requested. Allowed number of cores for %v is: %v",
			cpuSeries,
			tl.allowedCores,
		)
	}

	// Memory must be a multiple of 256 MB
	if memory%256 != 0 {
		return "", fmt.Errorf("requested memory must be a multiple of 256 MB")
	}

	// Check if the requested memory isn't too little
	if memory < coreCount*tl.minMemPerCore {
		return "", fmt.Errorf(
			"requested memory is too low. Minimal memory for %v is %v MB per core",
			cpuSeries,
			tl.minMemPerCore,
		)
	}

	// Check if the requested memory isn't too much
	if memory > coreCount*tl.maxMemPerCore && !tl.allowExtraMemory {
		return "", fmt.Errorf(
			"requested memory is too large.. Maximum memory allowed for %v is %v MB per core",
			cpuSeries,
			tl.maxMemPerCore,
		)
	}
	if memory > tl.extraMemoryLimit && tl.allowExtraMemory {
		return "", fmt.Errorf(
			"requested memory is too large.. Maximum memory allowed for %v is %v MB",
			cpuSeries,
			tl.extraMemoryLimit,
		)
	}

	// Return the custom machine type in form of a string acceptable by Compute Engine API.
	if containsString([]string{e2Small, e2Micro, e2Medium}, cpuSeries) {
		return fmt.Sprintf("zones/%v/machineTypes/%v-%v", zone, cpuSeries, memory), nil
	}

	if memory > coreCount*tl.maxMemPerCore {
		return fmt.Sprintf(
			"zones/%v/machineTypes/%v-%v-%v-ext",
			zone,
			cpuSeries,
			coreCount,
			memory,
		), nil
	}

	return fmt.Sprintf("zones/%v/machineTypes/%v-%v-%v", zone, cpuSeries, coreCount, memory), nil
}

func CreateVM(
	ctx context.Context,
	instanceName, pubKey, project, zone, region, registryBranch, gitLabBranch string,
	instancesClient *compute.InstancesClient,
	additionalVMLabels map[string]string,
	imageName, deploymentType string,
	eeVersion, noDeploy bool,
) error {
	machineType, err := customMachineTypeURI(zone, "e2-custom", 8, 16384)
	if err != nil {
		return fmt.Errorf("unable to create custom machine type string: %w", err)
	}

	req := &computepb.InsertInstanceRequest{
		Project: project,
		Zone:    zone,
		InstanceResource: &computepb.Instance{
			Name: proto.String(instanceName),
			Labels: map[string]string{
				"devvm": "true",
			},
			Metadata: &computepb.Metadata{
				Items: []*computepb.Items{
					{
						Key:   proto.String("registry_branch"),
						Value: proto.String(registryBranch),
					},
					{
						Key:   proto.String("gitlab_branch"),
						Value: proto.String(gitLabBranch),
					},
					{
						Key:   proto.String("deployment_type"),
						Value: proto.String(deploymentType),
					},
					{
						Key:   proto.String("ee_version"),
						Value: proto.String(fmt.Sprintf("%t", eeVersion)),
					},
					{
						Key:   proto.String("no_deploy"),
						Value: proto.String(fmt.Sprintf("%t", noDeploy)),
					},
					// NOTE(prozlach): This is intentional, we need
					// manually-managed ssh-keys in order to be able to run e2e
					// tests. In theory we could try to marry os-login with
					// Severice Account that e2e is used, but not in this
					// iteration.
					{
						Key:   proto.String("enable-oslogin"),
						Value: proto.String("false"),
					},
					{
						Key:   proto.String("ssh-keys"),
						Value: proto.String(fmt.Sprintf("dev:%s", pubKey)),
					},
				},
			},
			Disks: []*computepb.AttachedDisk{
				{
					InitializeParams: &computepb.AttachedDiskInitializeParams{
						DiskSizeGb: proto.Int64(100),
						SourceImage: proto.String(
							fmt.Sprintf("projects/%s/global/images/%s", constants.GCEProject, imageName),
						),
						DiskType: proto.String(fmt.Sprintf("zones/%s/diskTypes/pd-ssd", zone)),
					},
					AutoDelete: proto.Bool(true),
					Boot:       proto.Bool(true),
					Type:       proto.String(computepb.AttachedDisk_PERSISTENT.String()),
					DeviceName: proto.String("root-disk"),
					Mode:       proto.String("rw"),
				},
			},
			MachineType: proto.String(machineType),
			NetworkInterfaces: []*computepb.NetworkInterface{
				{
					Subnetwork: proto.String(fmt.Sprintf("projects/%s/regions/%s/subnetworks/devvm-vpc", project, region)),
					AccessConfigs: []*computepb.AccessConfig{
						{
							Name:        proto.String("External NAT"),
							Type:        proto.String("ONE_TO_ONE_NAT"),
							NetworkTier: proto.String("PREMIUM"),
						},
					},
				},
			},
		},
	}

	for k, v := range additionalVMLabels {
		req.InstanceResource.Labels[k] = v
	}

	op, err := instancesClient.Insert(ctx, req, DefaultRetryOption())
	if err != nil {
		return fmt.Errorf("unable to create instance: %w", err)
	}

	if err = op.Wait(ctx); err != nil {
		return fmt.Errorf("unable to wait for the operation: %w", err)
	}

	return nil
}
