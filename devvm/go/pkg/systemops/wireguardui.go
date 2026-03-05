package systemops

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/moby/moby/pkg/namesgenerator"
	"github.com/sethvargo/go-password/password"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"go.uber.org/zap"
)

func ensureWireguardUI(logger *zap.SugaredLogger) status.PartialStatus {
	publicIP, err := getPublicIP(logger)
	if err != nil {
		return status.Failed("unable to determine instance's public IP: %v", err)
	}

	user, pass, configUpdated, err := ensureWireguardUICreds(logger, publicIP)
	if err != nil {
		return status.Failed("unable to ensure wireguard-ui credentials: %v", err)
	}

	if err := ensureWireguardUIService(logger, configUpdated); err != nil {
		return status.Failed("unable to ensure wireguard-ui service: %v", err)
	}

	logger.Info("wireguard-ui has been set-up")
	return buildWireguardUISuccessStatus(publicIP, user, pass)
}

// buildWireguardUISuccessStatus creates the success status for Wireguard UI setup
func buildWireguardUISuccessStatus(publicIP, user, pass string) status.PartialStatus {
	return status.PartialStatus{
		Succeeded: status.StatusStageOK,
		DetailedMessage: []string{
			fmt.Sprintf("ui is now reachable at: https://%s:6000/", publicIP),
			fmt.Sprintf("username: %s", user),
			fmt.Sprintf("password: %s", pass),
		},
	}
}

func getPublicIP(logger *zap.SugaredLogger) (string, error) {
	gceHeaders := map[string]string{
		"Metadata-Flavor": "Google",
	}

	ctx, cancelF := context.WithTimeout(
		context.Background(),
		time.Duration(constants.DefaultK8sOperationTimeout)*time.Second,
	)
	defer cancelF()

	logger.Debug("fetching instance's public IP")
	url := "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip"
	resp, respStatus, err := httpGetReq(ctx, logger, url, gceHeaders, false)
	if err != nil {
		return "", fmt.Errorf("http GET request failed: %w", err)
	}
	if respStatus != http.StatusOK {
		return "", fmt.Errorf("http GET request returned status %d", respStatus)
	}
	return string(resp), nil
}

func ensureWireguardUIService(logger *zap.SugaredLogger, restartIfRunning bool) error {
	isRunning, err := isServiceRunning(logger, constants.WireguardUIServiceName)
	if err != nil {
		return fmt.Errorf("determining if wireguard UI is running: %w", err)
	}

	if isRunning && !restartIfRunning {
		logger.Debug("wireguard-ui service was already up2date, no need to start/restart")
		return nil
	}

	if isRunning && restartIfRunning {
		if err := stopService(logger, constants.WireguardUIServiceName); err != nil {
			return fmt.Errorf("stopping wireguard ui service: %w", err)
		}

		if err := os.RemoveAll(constants.WireguardUIStatePath); err != nil {
			return fmt.Errorf("removing old wireguard-ui state: %w", err)
		}
	}

	if err := startService(logger, constants.WireguardUIServiceName); err != nil {
		return fmt.Errorf("starting wireguard ui service: %w", err)
	}

	if err := enableService(logger, constants.WireguardUIServiceName); err != nil {
		return fmt.Errorf("enabling wireguard ui service: %w", err)
	}

	logger.Debug("wireguard-ui service have been ensured")
	return nil
}

func ensureWireguardUICreds(logger *zap.SugaredLogger, publicIP string) (string, string, bool, error) {
	confBytes, err := os.ReadFile(constants.WireguardUIConfPath)
	if err != nil {
		return "", "", false, fmt.Errorf("unable to read conf file %s: %w", constants.WireguardUIConfPath, err)
	}
	conf := string(confBytes)

	configUpdated := false

	// Extract or generate username
	name, conf, updated, err := extractOrGenerateWireguardCredential(
		conf, `INIT_USERNAME=(\w+)`, constants.WireguardUIUsernamePlaceholder,
		func() string { return namesgenerator.GetRandomName(0) },
	)
	if err != nil {
		return "", "", false, fmt.Errorf("wireguard-ui username extraction failed: %w", err)
	}
	if updated {
		configUpdated = true
		logger.Info("wireguard-ui username has been updated")
	}

	// Extract or generate password
	pass, conf, updated, err := extractOrGenerateWireguardCredential(
		conf, `INIT_PASSWORD=(\w+)`, constants.WireguardUIPasswordPlaceholder,
		func() string {
			p, err := password.Generate(32, 10, 0, false, true)
			if err != nil {
				return "", "", false, fmt.Errorf("failed to generate password: %w", err)
			}
			return p
		},
	)
	if err != nil {
		return "", "", false, fmt.Errorf("wireguard-ui password extraction failed: %w", err)
	}
	if updated {
		configUpdated = true
		logger.Info("wireguard-ui password has been updated")
	}

	// Extract or set public IP
	_, conf, updated, err = extractOrGenerateWireguardCredential(
		conf, `INIT_HOST=([\w.]+)`, constants.WireguardUIPublicIPPlaceholder,
		func() string { return publicIP },
	)
	if err != nil {
		return "", "", false, fmt.Errorf("wireguard-ui public IP extraction failed: %w", err)
	}
	if updated {
		configUpdated = true
		logger.Info("wireguard-ui public IP has been updated")
	}

	if configUpdated {
		if err := os.WriteFile(constants.WireguardUIConfPath, []byte(conf), 0o600); err != nil {
			return "", "", false, fmt.Errorf("unable to write conf file %s: %w", constants.WireguardUIConfPath, err)
		}
	}

	logger.Debug("wireguard-ui credentials have been ensured")
	return name, pass, configUpdated, nil
}

// extractOrGenerateWireguardCredential extracts a credential from config or generates a new one
func extractOrGenerateWireguardCredential(
	conf, pattern, placeholder string,
	generator func() string,
) (string, string, bool, error) {
	re := regexp.MustCompile(pattern)
	submatches := re.FindStringSubmatch(conf)

	if submatches == nil || len(submatches) != 2 {
		return "", "", false, fmt.Errorf("config file is malformed, submatches: %v", submatches)
	}

	if submatches[1] == placeholder {
		value := generator()
		conf = strings.Replace(conf, placeholder, value, 1)
		return value, conf, true, nil
	}

	return submatches[1], conf, false, nil
}
