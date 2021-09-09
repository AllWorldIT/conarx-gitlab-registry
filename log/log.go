package log

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

// Logger provides a leveled-logging interface.
type Logger interface {
	// standard logger methods
	Print(args ...interface{})

	Fatal(args ...interface{})
	Fatalln(args ...interface{})

	Panic(args ...interface{})

	// Leveled methods, from logrus
	Trace(args ...interface{})

	Debug(args ...interface{})

	Error(args ...interface{})

	Info(args ...interface{})

	Warn(args ...interface{})

	WithError(error) Logger
	WithFields(Fields) Logger
}

// LoggerKey is exposed temporarily while we're in the process of moving the
// legacy logging over to this package.
type LoggerKey struct{}

// Fields is an alias so that callers only need to know about this package
type Fields = logrus.Fields

type wrapper struct {
	*logrus.Entry
}

// FromLogrusLogger converts a logrus.Logger into Logger.
func FromLogrusLogger(l *logrus.Logger) Logger {
	return &wrapper{logrus.NewEntry(l)}
}

// ToLogrusEntry converts a Logger into a logrus.Entry. Useful for testing.
func ToLogrusEntry(l Logger) (*logrus.Entry, error) {
	wrapper, ok := l.(*wrapper)
	if !ok {
		return nil, errors.New("base logger is not a wrapper")
	}

	return wrapper.Entry, nil
}

func (w *wrapper) WithError(err error) Logger {
	return &wrapper{w.Entry.WithError(err)}
}

func (w *wrapper) WithFields(f Fields) Logger {
	return &wrapper{w.Entry.WithFields(f)}
}

// WithLogger creates a new context with provided logger.
func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, LoggerKey{}, logger)
}

type logOptions struct {
	ctx  context.Context
	keys []interface{}
}

type logOpt func(o *logOptions)

// WithContext returns the logger from the current context, if present.
func WithContext(ctx context.Context) logOpt {
	return func(o *logOptions) {
		o.ctx = ctx
	}
}

// WithKeys allows the passing of one or more log keys. They will be resolved on
// the logger's context and included in the logger. While context.Value takes an
// interface, any key argument passed to GetLogger will be passed to fmt.Sprint
// when expanded as a logging key field. If context keys are integer constants,
// for example, its recommended that a String method is implemented.
func WithKeys(keys ...interface{}) logOpt {
	return func(o *logOptions) {
		o.keys = keys
	}
}

// GetLogger returns a Logger based on a logrus Entry.
func GetLogger(opts ...logOpt) Logger {
	cfg := &logOptions{ctx: context.Background()}
	for _, o := range opts {
		o(cfg)
	}

	return &wrapper{getLogrusLogger(cfg.ctx, cfg.keys...)}
}

// GetLogrusLogger returns the logrus logger for the context. If one more keys
// are provided, they will be resolved on the context and included in the
// logger. Only use this function if specific logrus functionality is
// required.
func getLogrusLogger(ctx context.Context, keys ...interface{}) *logrus.Entry {
	var logger *logrus.Entry

	// Get a logger, if it is present.
	loggerInterface := ctx.Value(LoggerKey{})
	if loggerInterface != nil {
		if lgr, ok := loggerInterface.(*logrus.Entry); ok {
			logger = lgr
		}
	}

	if logger == nil {
		fields := logrus.Fields{}

		// Fill in the instance id, if we have it.
		instanceID := ctx.Value("instance.id")
		if instanceID != nil {
			fields["instance_id"] = instanceID
		}

		fields["go_version"] = runtime.Version()
		// If no logger is found, just return the standard logger.
		logger = logrus.StandardLogger().WithFields(fields)
	}

	fields := logrus.Fields{}
	for _, key := range keys {
		v := ctx.Value(key)
		if v != nil {
			fields[standardizedKey(fmt.Sprint(key))] = v
		}
	}

	return logger.WithFields(fields)
}

// standardizedKey converts all dots to underscores in key names in order to enforce a consistent naming convention
// across application and access logs. This is run only once per logger and key names are static and short, so there are
// no performance concerns.
func standardizedKey(key string) string {
	return strings.ReplaceAll(key, ".", "_")
}
