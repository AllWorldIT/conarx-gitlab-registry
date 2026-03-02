package systemops

import (
	"encoding/json"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"go.uber.org/zap"
)

type NetworkInfo struct {
	SVCIP         string `json:"svc_ip"`
	VIPRangeEnd   string `json:"vip_range_end"`
	VIPRangeStart string `json:"vip_range_start"`
}

func readNetworkInfo(
	logger *zap.SugaredLogger,
) (
	string, string, string, status.PartialStatus,
) {
	logger.Infof("retrieving network information from %s", constants.DefaultNetworkInfoFilePath)
	b, err := os.ReadFile(constants.DefaultNetworkInfoFilePath)
	if err != nil {
		res := status.Failed(
			"unable to read networking info file %s: %v", constants.DefaultNetworkInfoFilePath, err,
		)
		return "", "", "", res
	}

	tmp := new(NetworkInfo)

	err = json.Unmarshal(b, tmp)
	if err != nil {
		res := status.Failed("unable to unmarshal network info: %v", err)
		return "", "", "", res
	}

	res := status.PartialStatus{
		Succeeded: status.StatusStageOK,
		DetailedMessage: []string{
			fmt.Sprintf("SVC IP: %s", tmp.SVCIP),
			fmt.Sprintf("K8s VIP range start: %s", tmp.VIPRangeStart),
			fmt.Sprintf("K8s VIP range end: %s", tmp.VIPRangeEnd),
		},
		Metadata: map[string]string{
			"svc_ip": tmp.SVCIP,
		},
	}
	return tmp.SVCIP, tmp.VIPRangeStart, tmp.VIPRangeEnd, res
}
