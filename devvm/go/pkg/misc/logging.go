// nolint: revive // I like name `misc`
package misc

import (
	"fmt"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	cr_log "sigs.k8s.io/controller-runtime/pkg/log"
)

// LoggerWithCRLog creates a logger and sets up controller-runtime logging
func LoggerWithCRLog(logLevel string, logPretty bool) (*zap.SugaredLogger, func(), error) {
	logger, syncF, err := Logger(logLevel, logPretty)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to instantiate logger: %w", err)
	}

	cr_log.SetLogger(zapr.NewLogger(logger.Desugar()))

	return logger, syncF, nil
}

func Logger(logLevel string, logPretty bool) (*zap.SugaredLogger, func(), error) {
	level, err := zap.ParseAtomicLevel(logLevel)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse loglevel %q: %w", logLevel, err)
	}

	config := &zap.Config{
		Level:            level,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	if logPretty {
		config.Encoding = "console"
		config.EncoderConfig = zapcore.EncoderConfig{
			MessageKey:  "message",
			LevelKey:    "level",
			TimeKey:     "time",
			EncodeLevel: zapcore.LowercaseColorLevelEncoder,
			EncodeTime:  zapcore.RFC3339TimeEncoder,
		}
	} else {
		config.Encoding = "json"
		config.EncoderConfig = zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "lvl",
			MessageKey:     "msg",
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.EpochTimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}
	}

	logger, err := config.Build()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create logger: %w", err)
	}

	sync := func() {
		_ = logger.Sync()
	}

	return logger.Sugar(), sync, nil
}
