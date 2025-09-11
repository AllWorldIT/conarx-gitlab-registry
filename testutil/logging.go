package testutil

import (
	"context"
	"testing"

	dcontext "github.com/docker/distribution/context"
	"github.com/sirupsen/logrus"
)

type logWriterType struct {
	t testing.TB
}

func (l logWriterType) Write(p []byte) (n int, err error) {
	l.t.Log(string(p))
	return len(p), nil
}

func NewContextWithLogger(tb testing.TB) context.Context {
	ctx := context.Background()

	logger := NewTestLogger(tb)
	ctx = dcontext.WithLogger(ctx, logger)

	return ctx
}

func NewTestLogger(t testing.TB) dcontext.Logger {
	logger := logrus.New().WithFields(
		logrus.Fields{
			"test": true,
		},
	)
	logger.Logger.Level = logrus.DebugLevel
	logger.Logger.SetOutput(logWriterType{t: t})
	return logger
}
