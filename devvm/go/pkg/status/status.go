package status

import (
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
	"time"

	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"go.uber.org/zap"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
)

type StatusData struct {
	PartialStatuses []PartialStatus
	AllDone         bool
}

type Status struct {
	StatusData

	lock           sync.RWMutex
	logger         *zap.SugaredLogger
	statusFilePath string
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

// Failed creates a PartialStatus indicating failure
func Failed(format string, args ...any) PartialStatus {
	return PartialStatus{
		Succeeded:       StatusStageFailed,
		DetailedMessage: []string{fmt.Sprintf(format, args...)},
	}
}

func (s *Status) MarkAllDone() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.AllDone = true

	s.save()
}

func (s *Status) Upsert(ps PartialStatus) {
	s.lock.Lock()
	defer s.lock.Unlock()

	found := false
	for i := range s.PartialStatuses {
		if s.PartialStatuses[i].Name == ps.Name {
			s.PartialStatuses[i] = ps
			found = true
		}
	}
	if !found {
		s.PartialStatuses = append(s.PartialStatuses, ps)
	}

	// Keep the status up-to-date to make sure the user always knows at what
	// state we are.
	s.save()
}

func (s *Status) save() {
	// NOTE(prozlach): Not sure if we shouldn't propagate error to the caller,
	// instead of just logging them with ERROR severity. OTOH this would
	// require lots of boilerplate in the code to handle these errors.
	b, err := json.Marshal(s.StatusData)
	if err != nil {
		s.logger.Errorf("unable to marschal status: %w", err)
	}
	tmpPath := s.statusFilePath + ".tmp"
	// nolint: gosec // this file needs to be shared with `dev` user
	err = os.WriteFile(tmpPath, b, 0o640)
	if err != nil {
		s.logger.Errorf("unable to write marschaled status to %s: %w", tmpPath, err)
	}

	// Change group ownership to `dev`
	group, err := user.LookupGroup(constants.DevvmUser)
	if err != nil {
		s.logger.Errorf("unable to lookup group 'dev': %w", err)
	} else {
		gid, err := strconv.Atoi(group.Gid)
		if err != nil {
			s.logger.Errorf("unable to parse gid: %w", err)
		} else {
			// -1 means "don't change owner"
			err = os.Chown(tmpPath, -1, gid)
			if err != nil {
				s.logger.Errorf("unable to chown status file to group %q: %w", constants.DevvmUser, err)
			}
		}
	}

	err = os.Rename(tmpPath, s.statusFilePath)
	if err != nil {
		s.logger.Errorf("unable to move status file to its final destination: %w", err)
	}
	s.logger.Debugf("status saved successfully to %s", s.statusFilePath)
}

func (s *Status) Print(printfF func(string, ...any) (int, error)) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, _ = printfF("Booter status:\n")
	maxLen := 0
	for _, ps := range s.PartialStatuses {
		if maxLen < len(ps.Name) {
			maxLen = len(ps.Name)
		}
	}

	allDone := true

	for _, ps := range s.PartialStatuses {
		var color string
		var txt string
		switch ps.Succeeded {
		case StatusStageFailed:
			color = colorRed
			txt = "FAILED"
			allDone = false
		case StatusStageOK:
			color = colorGreen
			txt = "OK"
		case StatusStageInProgress:
			color = colorYellow
			txt = "IN PROGRESS"
			allDone = false
		}
		// TODO(prozlach): make it nicer using github.com/buger/goterm
		_, _ = printfF("%s:\t\r\x1b[%dC%s[%s]%s\n", ps.Name, maxLen+4, color, txt, colorReset)
		for _, ln := range ps.DetailedMessage {
			_, _ = printfF("\t%s\n", ln)
		}
		_, _ = printfF("\tStage retries count: %d\n", ps.RetriesCount)
		_, _ = printfF("\tStage total time: %s\n", ps.RunDuration.String())
	}

	_, _ = printfF("\n")

	if allDone {
		_, _ = printfF("All tasks were successfully completed!\n")
		return
	}

	_, _ = printfF("Devvm is still being boostrapped.\n")
	_, _ = printfF("More information on what is happening can be obtained by issuing `journalctl -fu booter.service` as root.\n")
}

func New(logger *zap.SugaredLogger, statusFilePath string) *Status {
	return &Status{
		StatusData: StatusData{
			PartialStatuses: make([]PartialStatus, 0),
			AllDone:         false,
		},
		lock:           sync.RWMutex{},
		logger:         logger,
		statusFilePath: statusFilePath,
	}
}

func (s *Status) Load() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	b, err := os.ReadFile(s.statusFilePath)
	if err != nil {
		return fmt.Errorf("unable to read status file %s: %w", s.statusFilePath, err)
	}
	err = json.Unmarshal(b, &s.StatusData)
	if err != nil {
		return fmt.Errorf("unable to unmarschal status: %w", err)
	}
	return nil
}
