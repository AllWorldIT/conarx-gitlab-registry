package testutil

import (
	"context"
	"testing"

	dcontext "github.com/docker/distribution/context"
	"github.com/sirupsen/logrus"
)

type logWriterType struct {
	t *testing.T
}

func (l logWriterType) Write(p []byte) (n int, err error) {
	l.t.Logf(string(p))
	return len(p), nil
}

func NewContextWithLogger(t *testing.T) context.Context {
	ctx := context.Background()

	logger := logrus.New().WithFields(logrus.Fields{
		"test": true,
	})
	logger.Logger.Level = logrus.DebugLevel
	logger.Logger.SetOutput(logWriterType{t: t})
	ctx = dcontext.WithLogger(ctx, logger)

	return ctx
}
