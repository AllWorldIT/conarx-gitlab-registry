package status

import (
	"fmt"
	"time"
)

type StatusData struct {
	PartialStatuses []PartialStatus
	AllDone         bool
}

type Status struct {
	StatusData
}

type StatusStage string

const (
	StatusStageFailed     StatusStage = "failed"
	StatusStageInProgress StatusStage = "inProgress"
	StatusStageOK         StatusStage = "ok"
)

type PartialStatus struct {
	Name            string
	Succeeded       StatusStage
	DetailedMessage []string
	RetriesCount    int
	RunDuration     time.Duration
	Metadata        map[string]string
}

func (sd *StatusData) StatusSummary() (bool, []string) {
	if len(sd.PartialStatuses) == 0 {
		panic("partial statuses list is empty")
	}

	if sd.AllDone {
		return true, nil
	}

	res := make([]string, 0)
	for _, ps := range sd.PartialStatuses {
		if ps.Succeeded != StatusStageOK {
			msg := fmt.Sprintf(
				"stage: %s, status: %s, run duration: %s, retries count: %d",
				ps.Name, ps.Succeeded, ps.RunDuration.String(), ps.RetriesCount,
			)
			res = append(res, msg)
		}
	}

	// New task has not started yet, all other tasks succeeded. Let's wait a
	// bit.
	return false, res
}
