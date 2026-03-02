package userops

import (
	"fmt"
	"time"

	tm "github.com/buger/goterm"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"go.uber.org/zap"
)

func Do(logger *zap.SugaredLogger, watch bool) error {
	st := status.New(logger, constants.StatusFilePath)

	if !watch {
		if err := st.Load(); err != nil {
			return fmt.Errorf("unable to load status file: %w", err)
		}

		st.Print(fmt.Printf)
		return nil
	}

	for {
		if err := st.Load(); err != nil {
			return fmt.Errorf("unable to load status file: %w", err)
		}

		tm.Clear()
		tm.MoveCursor(1, 1)
		st.Print(tm.Printf)
		tm.Flush()
		time.Sleep(time.Second)
	}
}
