package context

import (
	"context"
	"fmt"
	"runtime"
	"strings"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/version"
	"github.com/sirupsen/logrus"
)

// Logger provides a leveled-logging interface.
type Logger interface {
	// standard logger methods
	Print(args ...any)
	Printf(format string, args ...any)
	Println(args ...any)

	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Fatalln(args ...any)

	Panic(args ...any)
	Panicf(format string, args ...any)
	Panicln(args ...any)

	// Leveled methods, from logrus
	Trace(args ...any)
	Tracef(format string, args ...any)
	Traceln(args ...any)

	Debug(args ...any)
	Debugf(format string, args ...any)
	Debugln(args ...any)

	Error(args ...any)
	Errorf(format string, args ...any)
	Errorln(args ...any)

	Info(args ...any)
	Infof(format string, args ...any)
	Infoln(args ...any)

	Warn(args ...any)
	Warnf(format string, args ...any)
	Warnln(args ...any)

	WithError(err error) *logrus.Entry
	WithField(key string, value any) *logrus.Entry
	WithFields(fields logrus.Fields) *logrus.Entry
}

// WithLogger creates a new context with provided logger.
func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, log.LoggerKey{}, logger)
}

// GetLoggerWithField returns a logger instance with the specified field key
// and value without affecting the context. Extra specified keys will be
// resolved from the context.
func GetLoggerWithField(ctx context.Context, key, value any, keys ...any) Logger {
	return getLogrusLogger(ctx, keys...).WithField(fmt.Sprint(key), value)
}

// GetLoggerWithFields returns a logger instance with the specified fields
// without affecting the context. Extra specified keys will be resolved from
// the context.
func GetLoggerWithFields(ctx context.Context, fields map[any]any, keys ...any) Logger {
	// must convert from any -> any to string -> any for logrus.
	lfields := make(logrus.Fields, len(fields))
	for key, value := range fields {
		lfields[fmt.Sprint(key)] = value
	}

	return getLogrusLogger(ctx, keys...).WithFields(lfields)
}

// GetLogger returns the logger from the current context, if present. If one
// or more keys are provided, they will be resolved on the context and
// included in the logger. While context.Value takes an interface, any key
// argument passed to GetLogger will be passed to fmt.Sprint when expanded as
// a logging key field. If context keys are integer constants, for example,
// its recommended that a String method is implemented.
func GetLogger(ctx context.Context, keys ...any) Logger {
	return getLogrusLogger(ctx, keys...)
}

// GetLogrusLogger returns the logrus logger for the context. If one more keys
// are provided, they will be resolved on the context and included in the
// logger. Only use this function if specific logrus functionality is
// required.
func getLogrusLogger(ctx context.Context, keys ...any) *logrus.Entry {
	var logger *logrus.Entry

	// Get a logger, if it is present.
	loggerInterface := ctx.Value(log.LoggerKey{})
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
		fields["version"] = version.Version

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
