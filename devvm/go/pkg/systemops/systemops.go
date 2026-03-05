package systemops

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/egymgmbh/go-prefix-writer/prefixer"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"go.uber.org/zap"
	"go.uber.org/zap/zapio"
)

type taskT struct {
	name string
	f    func() status.PartialStatus
}

func Do(logger *zap.SugaredLogger) error {
	logger.Info("Ensuring this VM is bootstrapped")
	st := status.New(logger, constants.StatusFilePath)

	var svcIP string
	var vmLabels VMLabels

	runConcurently(
		logger, st,
		taskT{
			constants.VMlabelsStatusName,
			func() status.PartialStatus {
				var res status.PartialStatus

				vmLabels, res = inspectVMLabels(logger)
				return res
			},
		},
		taskT{
			constants.NetworkInfoStageName,
			func() status.PartialStatus {
				var res status.PartialStatus

				svcIP, _, _, res = readNetworkInfo(logger)
				return res
			},
		},
	)

	logger.Infow("Using deployment type from VM metadata", "deploymentType", vmLabels.DeploymentType, "eeVersion", vmLabels.EEVersion)

	firstBatchTasks := []taskT{
		{
			constants.WireguardVPNStageName,
			func() status.PartialStatus {
				return ensureWireguardUI(logger)
			},
		},
		{
			constants.TLSCertsStageName,
			func() status.PartialStatus {
				var res status.PartialStatus

				_, res = generateTLSCertificates(logger, svcIP)
				return res
			},
		},
	}

	runConcurently(logger, st, firstBatchTasks...)

	st.MarkAllDone()

	return nil
}

func runConcurently(logger *zap.SugaredLogger, st *status.Status, tasks ...taskT) {
	wg := new(sync.WaitGroup)

	for _, task := range tasks {
		wg.Add(1)

		logger.Infow("starting task", "name", task.name)
		go func(t taskT) {
			defer wg.Done()

			retryUntilSuccessful(logger, t.name, st, t.f)
			logger.Infow("task done", "name", t.name)
		}(task)
	}

	wg.Wait()
}

func retryUntilSuccessful(
	logger *zap.SugaredLogger,
	name string,
	st *status.Status,
	f func() status.PartialStatus,
) {
	tStart := time.Now()
	nTries := -1

	initialRes := status.PartialStatus{
		Name:      name,
		Succeeded: status.StatusStageInProgress,
		Metadata:  make(map[string]string),
	}
	st.Upsert(initialRes)

	for {
		res := f()

		nTries++
		tDuration := time.Since(tStart)

		res.Name = name
		res.RetriesCount = nTries
		res.RunDuration = tDuration
		st.Upsert(res)

		if res.Succeeded == status.StatusStageOK {
			break
		}

		logger.Errorf("stage %q in state %q: %s", name, res.Succeeded, strings.Join(res.DetailedMessage, ";"))

		// NOTE(prozlach): dumb sleep to prevent tight retry-loops. NOt sure if
		// something smarter is really needed.
		time.Sleep(time.Second)
	}
}

func executeCmd(logger *zap.SugaredLogger, cwd string, cmdStr ...string) (string, int, error) {
	debugwriter := &zapio.Writer{
		Log:   logger.Desugar(),
		Level: zap.DebugLevel,
	}
	defer debugwriter.Close()

	prefixedWriter := prefixer.New(debugwriter, func() string { return "CMD_OUT | " })

	buf := new(bytes.Buffer)
	outWriter := io.MultiWriter(buf, prefixedWriter)

	logger.Debugw("executeCmd", "cmdStr", cmdStr, "cwd", cwd)

	// nolint: gosec // we fully control the input here, its a helper function
	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	cmd.Stdout = outWriter
	cmd.Stderr = outWriter
	if cwd != "" {
		cmd.Dir = cwd
	}

	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return buf.String(), exitErr.ExitCode(), nil
		}
		return "", -1, fmt.Errorf("failed to run command %q: %w", cmdStr, err)
	}
	return buf.String(), 0, nil
}

// executeCmdAsDevWithBash is an opinionated version of executeCmd()
// nolint:unparam // cwd can be useful in future iterations
func executeCmdAsDevWithBash(logger *zap.SugaredLogger, cwd, cmd, msg string) (string, error) {
	logger.Infof(msg)

	if cwd == "" {
		cwd = "/home/dev/"
	}

	out, exitStatus, err := executeCmd(
		logger, cwd,
		"sudo", "-i", "-u", constants.DevvmUser, "/bin/bash", "-l", "-c", "eval \"$(mise activate bash)\";"+cmd,
	)
	if err != nil {
		return "", fmt.Errorf("failed to execute `%s`: %w", cmd, err)
	}
	if exitStatus != 0 {
		return "", fmt.Errorf("`%s` returned non-zero exit status %d", cmd, exitStatus)
	}

	return out, nil
}

func httpGetReq(
	ctx context.Context,
	logger *zap.SugaredLogger,
	url string,
	headers map[string]string,
	insecureSkipVerify bool,
) ([]byte, int, error) {
	logger.Debugf("fetching url %s", url)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create new http req for URL %q: %w", url, err)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			// nolint: gosec // we are using self-signed certs
			InsecureSkipVerify: insecureSkipVerify,
		},
	}
	hclient := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	res, err := hclient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute http req for URL %q: %w", url, err)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to read response body when fetching URL %q: %w", url, err)
	}
	err = res.Body.Close()
	if err != nil {
		return nil, 0, fmt.Errorf("closing body: %w", err)
	}

	return body, res.StatusCode, nil
}

func isServiceRunning(logger *zap.SugaredLogger, name string) (bool, error) {
	out, _, err := executeCmd(logger, "", "systemctl", "is-active", name)
	if err != nil {
		return false, fmt.Errorf("unable to check if service %q is running: %w", name, err)
	}
	return strings.TrimSpace(out) == "active", nil
}

// systemctlAction runs a systemctl command and handles errors uniformly
func systemctlAction(logger *zap.SugaredLogger, action, serviceName string) error {
	out, exitCode, err := executeCmd(logger, "", "systemctl", action, serviceName)
	if err != nil {
		return fmt.Errorf("unable to %s service %q: %w", action, serviceName, err)
	}
	if exitCode != 0 {
		return fmt.Errorf("`systemctl %s %q` returned non-zero exit code: %d, output: %s",
			action, serviceName, exitCode, out)
	}
	return nil
}

func enableService(logger *zap.SugaredLogger, name string) error {
	return systemctlAction(logger, "enable", name)
}

func startService(logger *zap.SugaredLogger, name string) error {
	return systemctlAction(logger, "start", name)
}

func stopService(logger *zap.SugaredLogger, name string) error {
	return systemctlAction(logger, "stop", name)
}

// nolint: unused // still useful, will be used in future iterations
func restartService(logger *zap.SugaredLogger, name string) error {
	return systemctlAction(logger, "restart", name)
}
