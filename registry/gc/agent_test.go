package gc

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff/v4"
	"github.com/docker/distribution/registry/gc/internal"
	"github.com/docker/distribution/registry/gc/internal/mocks"
	"github.com/docker/distribution/registry/gc/worker"
	wmocks "github.com/docker/distribution/registry/gc/worker/mocks"
	regmocks "github.com/docker/distribution/registry/internal/mocks"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/labkit/correlation"
)

func TestNewAgent(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	tmp := logrus.New()
	tmp.SetOutput(io.Discard)
	defaultLogger := tmp.WithField(componentKey, agentName)

	tmp = logrus.New()
	customLogger := tmp.WithField(componentKey, agentName)

	type args struct {
		w    worker.Worker
		opts []AgentOption
	}
	tests := []struct {
		name string
		args args
		want *Agent
	}{
		{
			name: "defaults",
			args: args{
				w: workerMock,
			},
			want: &Agent{
				worker:          workerMock,
				logger:          defaultLogger,
				initialInterval: defaultInitialInterval,
				maxBackoff:      defaultMaxBackoff,
				noIdleBackoff:   false,
			},
		},
		{
			name: "with logger",
			args: args{
				w:    workerMock,
				opts: []AgentOption{WithLogger(customLogger)},
			},
			want: &Agent{
				worker:          workerMock,
				logger:          customLogger,
				initialInterval: defaultInitialInterval,
				maxBackoff:      defaultMaxBackoff,
				noIdleBackoff:   false,
			},
		},
		{
			name: "with initial interval",
			args: args{
				w:    workerMock,
				opts: []AgentOption{WithInitialInterval(10 * time.Hour)},
			},
			want: &Agent{
				worker:          workerMock,
				logger:          defaultLogger,
				initialInterval: 10 * time.Hour,
				maxBackoff:      defaultMaxBackoff,
				noIdleBackoff:   false,
			},
		},
		{
			name: "with max back off",
			args: args{
				w:    workerMock,
				opts: []AgentOption{WithMaxBackoff(10 * time.Hour)},
			},
			want: &Agent{
				worker:          workerMock,
				logger:          defaultLogger,
				initialInterval: defaultInitialInterval,
				maxBackoff:      10 * time.Hour,
				noIdleBackoff:   false,
			},
		},
		{
			name: "without idle back off",
			args: args{
				w:    workerMock,
				opts: []AgentOption{WithoutIdleBackoff()},
			},
			want: &Agent{
				worker:          workerMock,
				logger:          defaultLogger,
				initialInterval: defaultInitialInterval,
				maxBackoff:      defaultMaxBackoff,
				noIdleBackoff:   true,
			},
		},
		{
			name: "with all options",
			args: args{
				w: workerMock,
				opts: []AgentOption{
					WithLogger(customLogger),
					WithoutIdleBackoff(),
					WithInitialInterval(1 * time.Hour),
					WithMaxBackoff(2 * time.Hour),
					WithoutIdleBackoff(),
				},
			},
			want: &Agent{
				worker:          workerMock,
				logger:          customLogger,
				initialInterval: 1 * time.Hour,
				maxBackoff:      2 * time.Hour,
				noIdleBackoff:   true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewAgent(tt.args.w, tt.args.opts...)

			require.Equal(t, tt.want.worker, got.worker)
			require.Equal(t, tt.want.initialInterval, got.initialInterval)
			require.Equal(t, tt.want.maxBackoff, got.maxBackoff)
			require.Equal(t, tt.want.noIdleBackoff, got.noIdleBackoff)

			// we have to cast loggers and compare only their public fields
			wantLogger, ok := tt.want.logger.(*logrus.Entry)
			require.True(t, ok)
			gotLogger, ok := got.logger.(*logrus.Entry)
			require.True(t, ok)
			require.EqualValues(t, wantLogger.Logger.Level, gotLogger.Logger.Level)
			require.Equal(t, wantLogger.Logger.Formatter, gotLogger.Logger.Formatter)
			require.Equal(t, wantLogger.Logger.Out, gotLogger.Logger.Out)
		})
	}
}

func stubBackoff(tb testing.TB, m *mocks.MockBackoff) {
	tb.Helper()

	bkp := backoffConstructor
	backoffConstructor = func(initInterval, maxInterval time.Duration) internal.Backoff {
		return m
	}
	tb.Cleanup(func() { backoffConstructor = bkp })
}

func stubCorrelationID(tb testing.TB) string {
	tb.Helper()

	id := correlation.SafeRandomID()
	bkp := newCorrelationID
	newCorrelationID = func() string {
		return id
	}
	tb.Cleanup(func() { newCorrelationID = bkp })

	return id
}

func TestAgent_Start_Jitter(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	agent := NewAgent(workerMock,
		WithLogger(logrus.New()), // so that we can see the log output during test runs
	)

	// use fixed time for reproducible rand seeds (used to generate jitter durations)
	now := time.Time{}
	rand.Seed(now.UnixNano())
	expectedJitter := time.Duration(rand.Intn(startJitterMaxSeconds)) * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gomock.InOrder(
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Now().Return(now).Times(2), // backoff.NewExponentialBackOff calls Now() once
		clockMock.EXPECT().Sleep(expectedJitter).Do(func(_ time.Duration) {
			// cancel context here to avoid a subsequent worker run, which is not needed for the purpose of this test
			cancel()
		}).Times(1),
		clockMock.EXPECT().Now().Return(now).Times(1), // startQueueSizeMonitoring
	)

	err := agent.Start(ctx)
	require.NotNil(t, err)
	require.EqualError(t, context.Canceled, err.Error())
}

func TestAgent_Start_NoTaskFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	backoffMock := mocks.NewMockBackoff(ctrl)
	stubBackoff(t, backoffMock)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	agent := NewAgent(workerMock, WithLogger(logrus.New()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wCtx := correlation.ContextWithCorrelation(ctx, stubCorrelationID(t))

	seedTime := time.Time{}
	startTime := seedTime.Add(1 * time.Millisecond)
	backOff := defaultInitialInterval

	gomock.InOrder(
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Now().Return(seedTime).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(startTime).Times(1),
		workerMock.EXPECT().Name().Times(1),
		workerMock.EXPECT().Run(wCtx).Return(false, nil).Times(1),
		clockMock.EXPECT().Since(startTime).Return(100*time.Millisecond).Times(1),
		backoffMock.EXPECT().NextBackOff().Return(backOff).Times(1),
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Sleep(backOff).Do(func(_ time.Duration) { cancel() }).Times(1),
	)

	err := agent.Start(ctx)
	require.NotNil(t, err)
	require.EqualError(t, context.Canceled, err.Error())
}

func TestAgent_Start_NoTaskFoundWithoutIdleBackoff(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	backoffMock := mocks.NewMockBackoff(ctrl)
	stubBackoff(t, backoffMock)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	agent := NewAgent(workerMock, WithLogger(logrus.New()), WithoutIdleBackoff())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wCtx := correlation.ContextWithCorrelation(ctx, stubCorrelationID(t))

	seedTime := time.Time{}
	startTime := seedTime.Add(1 * time.Millisecond)
	backOff := defaultInitialInterval

	gomock.InOrder(
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Now().Return(seedTime).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(startTime).Times(1),
		workerMock.EXPECT().Name().Times(1),
		workerMock.EXPECT().Run(wCtx).Return(false, nil).Times(1),
		backoffMock.EXPECT().Reset().Times(1), // ensure backoff reset
		clockMock.EXPECT().Since(startTime).Return(100*time.Millisecond).Times(1),
		backoffMock.EXPECT().NextBackOff().Return(backOff).Times(1),
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Sleep(backOff).Do(func(_ time.Duration) { cancel() }).Times(1),
	)

	err := agent.Start(ctx)
	require.NotNil(t, err)
	require.EqualError(t, context.Canceled, err.Error())
}

func TestAgent_Start_RunFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	backoffMock := mocks.NewMockBackoff(ctrl)
	stubBackoff(t, backoffMock)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	agent := NewAgent(workerMock, WithLogger(logrus.New()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wCtx := correlation.ContextWithCorrelation(ctx, stubCorrelationID(t))

	seedTime := time.Time{}
	startTime := seedTime.Add(1 * time.Millisecond)
	backOff := defaultInitialInterval

	gomock.InOrder(
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Now().Return(seedTime).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(startTime).Times(1),
		workerMock.EXPECT().Name().Times(1),
		workerMock.EXPECT().Run(wCtx).Return(true, nil).Times(1),
		backoffMock.EXPECT().Reset().Times(1), // ensure backoff reset
		clockMock.EXPECT().Since(startTime).Return(100*time.Millisecond).Times(1),
		backoffMock.EXPECT().NextBackOff().Return(backOff).Times(1),
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Sleep(backOff).Do(func(_ time.Duration) { cancel() }).Times(1),
	)

	err := agent.Start(ctx)
	require.NotNil(t, err)
	require.EqualError(t, context.Canceled, err.Error())
}

func TestAgent_Start_RunError(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	backoffMock := mocks.NewMockBackoff(ctrl)
	stubBackoff(t, backoffMock)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	agent := NewAgent(workerMock, WithLogger(logrus.New()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wCtx := correlation.ContextWithCorrelation(ctx, stubCorrelationID(t))

	seedTime := time.Time{}
	startTime := seedTime.Add(1 * time.Millisecond)
	backOff := defaultInitialInterval

	gomock.InOrder(
		// there is no backoff reset here
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Now().Return(seedTime).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(startTime).Times(1),
		workerMock.EXPECT().Name().Times(1),
		workerMock.EXPECT().Run(wCtx).Return(false, errors.New("fake error")).Times(1),
		clockMock.EXPECT().Since(startTime).Return(100*time.Millisecond).Times(1),
		backoffMock.EXPECT().NextBackOff().Return(backOff).Times(1),
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Sleep(backOff).Do(func(_ time.Duration) { cancel() }).Times(1),
	)

	err := agent.Start(ctx)
	require.NotNil(t, err)
	require.EqualError(t, context.Canceled, err.Error())
}

func TestAgent_Start_RunLoopSurvivesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	backoffMock := mocks.NewMockBackoff(ctrl)
	stubBackoff(t, backoffMock)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	agent := NewAgent(workerMock, WithLogger(logrus.New()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wCtx := correlation.ContextWithCorrelation(ctx, stubCorrelationID(t))

	seedTime := time.Time{}
	startTime := seedTime.Add(1 * time.Millisecond)
	backOff := defaultInitialInterval

	gomock.InOrder(
		// 1st loop iteration
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Now().Return(seedTime).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(startTime).Times(1),
		workerMock.EXPECT().Name().Times(1),
		workerMock.EXPECT().Run(wCtx).Return(false, errors.New("fake error")).Times(1),
		clockMock.EXPECT().Since(startTime).Return(100*time.Millisecond).Times(1),
		backoffMock.EXPECT().NextBackOff().Return(backOff).Times(1),
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Sleep(backOff).Times(1),
		// 2nd loop iteration
		clockMock.EXPECT().Now().Return(startTime).Times(1),
		workerMock.EXPECT().Name().Times(1),
		workerMock.EXPECT().Run(wCtx).Return(true, nil).Times(1),
		backoffMock.EXPECT().Reset().Times(1), // ensure backoff reset
		clockMock.EXPECT().Since(startTime).Return(100*time.Millisecond).Times(1),
		backoffMock.EXPECT().NextBackOff().Return(backOff).Times(1),
		workerMock.EXPECT().Name().Times(1),
		clockMock.EXPECT().Sleep(backOff).Do(func(_ time.Duration) {
			// cancel context here to avoid a 3rd worker run
			cancel()
		}).Times(1),
	)

	err := agent.Start(ctx)
	require.NotNil(t, err)
	require.EqualError(t, context.Canceled, err.Error())
}

func stubQueueSizeMonitorInterval(tb testing.TB, d time.Duration) {
	tb.Helper()

	bkp := queueSizeMonitorInterval
	queueSizeMonitorInterval = d
	tb.Cleanup(func() { queueSizeMonitorInterval = bkp })
}

func TestAgent_startQueueSizeMonitoring(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	monitorInterval := 10 * time.Millisecond
	stubQueueSizeMonitorInterval(t, monitorInterval)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	backoffMock := mocks.NewMockBackoff(ctrl)
	stubBackoff(t, backoffMock)

	log := logrus.New()
	agent := NewAgent(workerMock, WithLogger(log))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Time{}
	queueSizeCtx := testutil.IsContextWithDeadline{Deadline: now.Add(queueSizeMonitorTimeout)}

	gomock.InOrder(
		// first
		clockMock.EXPECT().Now().Return(now).Times(1),
		workerMock.EXPECT().QueueSize(queueSizeCtx).Return(1, nil).Times(1),
		backoffMock.EXPECT().Reset().Times(1),
		workerMock.EXPECT().QueueName().Return("foo").Times(1),
		backoffMock.EXPECT().NextBackOff().Return(monitorInterval).Times(1),
		clockMock.EXPECT().Sleep(monitorInterval).Times(1),
		// second
		clockMock.EXPECT().Now().Return(now).Times(1),
		workerMock.EXPECT().QueueSize(queueSizeCtx).Return(0, nil).Times(1),
		backoffMock.EXPECT().Reset().Times(1),
		workerMock.EXPECT().QueueName().Return("foo").Times(1),
		backoffMock.EXPECT().NextBackOff().Return(monitorInterval).Times(1),
		clockMock.EXPECT().Sleep(monitorInterval).Times(1),
	)

	quit := agent.startQueueSizeMonitoring(ctx, log)
	require.NotNil(t, quit)

	done := make(chan struct{})
	time.AfterFunc(25*time.Millisecond, func() {
		quit.close()
		close(done)
	})
	<-done
}

func TestAgent_startQueueSizeMonitoring_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	monitorInterval := 10 * time.Millisecond
	backOff := monitorInterval
	stubQueueSizeMonitorInterval(t, monitorInterval)

	backoffMock := mocks.NewMockBackoff(ctrl)
	stubBackoff(t, backoffMock)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	log := logrus.New()
	agent := NewAgent(workerMock, WithLogger(log))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Time{}
	queueSizeCtx := testutil.IsContextWithDeadline{Deadline: now.Add(queueSizeMonitorTimeout)}

	gomock.InOrder(
		// first: query fail
		clockMock.EXPECT().Now().Return(now).Times(1),
		workerMock.EXPECT().QueueSize(queueSizeCtx).Return(0, errors.New("foo")).Times(1),
		backoffMock.EXPECT().NextBackOff().Return(backOff).Times(1),
		clockMock.EXPECT().Sleep(backOff).Times(1),
		// second: reset the backoff on success
		clockMock.EXPECT().Now().Return(now).Times(1),
		workerMock.EXPECT().QueueSize(queueSizeCtx).Return(0, nil).Times(1),
		backoffMock.EXPECT().Reset().Times(1),
		workerMock.EXPECT().QueueName().Return("foo").Times(1),
		backoffMock.EXPECT().NextBackOff().Return(monitorInterval).Times(1),
		clockMock.EXPECT().Sleep(monitorInterval).Times(1),
	)

	quit := agent.startQueueSizeMonitoring(ctx, log)
	require.NotNil(t, quit)

	done := make(chan struct{})
	time.AfterFunc(25*time.Millisecond, func() {
		quit.close()
		close(done)
	})
	<-done
}

func TestAgent_startQueueSizeMonitoring_Quit(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	monitorInterval := 20 * time.Millisecond
	stubQueueSizeMonitorInterval(t, monitorInterval)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	log := logrus.New()
	agent := NewAgent(workerMock, WithLogger(log))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// With an interval of 20ms, and closing the quit channel right after the initialization (below), this should be the
	// only call done
	gomock.InOrder(
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
	)

	quit := agent.startQueueSizeMonitoring(ctx, log)
	require.NotNil(t, quit)
	quit.close()
}

func TestAgent_startQueueSizeMonitoring_ContextDone(t *testing.T) {
	ctrl := gomock.NewController(t)
	workerMock := wmocks.NewMockWorker(ctrl)

	monitorInterval := 20 * time.Millisecond
	stubQueueSizeMonitorInterval(t, monitorInterval)

	clockMock := regmocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	log := logrus.New()
	agent := NewAgent(workerMock, WithLogger(log))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// With an interval of 20ms, and cancelling the context right after the initialization (below), this should be the
	// only call done
	gomock.InOrder(
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
	)

	quit := agent.startQueueSizeMonitoring(ctx, log)
	require.NotNil(t, quit)
	cancel()
}

func Test_newBackoff(t *testing.T) {
	clockMock := clock.NewMock()
	clockMock.Set(time.Time{})
	testutil.StubClock(t, &systemClock, clockMock)

	initInterval := 5 * time.Minute
	maxInterval := 24 * time.Hour

	want := &backoff.ExponentialBackOff{
		InitialInterval:     initInterval,
		RandomizationFactor: backoffJitterFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         maxInterval,
		MaxElapsedTime:      0,
		Stop:                backoff.Stop,
		Clock:               clockMock,
	}
	want.Reset()

	tmp := newBackoff(initInterval, maxInterval)
	got, ok := tmp.(*backoff.ExponentialBackOff)
	require.True(t, ok)
	require.NotNil(t, got)
	require.Equal(t, want, got)
}
