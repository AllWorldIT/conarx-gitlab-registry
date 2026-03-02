package systemops

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"go.uber.org/zap"
)

type taskT struct {
	name string
	f    func() status.PartialStatus
}

func Do(logger *zap.SugaredLogger) error {
	logger.Info("Ensuring this VM is bootstrapped")
	st := status.New(logger, constants.StatusFilePath)

	var svcIP string
	var vipMinAddress string
	var vipMaxAddress string
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

				svcIP, vipMinAddress, vipMaxAddress, res = readNetworkInfo(logger)
				return res
			},
		},
	)

	// NOTE(prozlach): temporary, just so that I can split changes into smaller
	// MRs.
	// nolint: dogsled // see above
	_, _, _ = svcIP, vipMinAddress, vipMaxAddress

	logger.Infow("Using deployment type from VM metadata", "deploymentType", vmLabels.DeploymentType, "eeVersion", vmLabels.EEVersion)

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
