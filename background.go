package app

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type BackgroundCommand struct {
	command                    Command
	interval                   time.Duration
	log                        log.Logger
	executionsCounter          metrics.Counter
	executionDurationHistogram metrics.Histogram
	stopped                    int32
}

func NewBackgroundCommand(cmd Command, opts ...BackgroundCommandOption) *BackgroundCommand {
	c := &BackgroundCommand{
		command:                    cmd,
		interval:                   time.Millisecond * 200,
		log:                        log.NewNopLogger(),
		executionsCounter:          discard.NewCounter(),
		executionDurationHistogram: discard.NewHistogram(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

type BackgroundCommandOption func(*BackgroundCommand)

// BackgroundWithInterval sets the time duration within command executions
func BackgroundCommandWithInterval(interval time.Duration) BackgroundCommandOption {
	return func(bg *BackgroundCommand) {
		bg.interval = interval
	}
}

func BackgroundCommandWithLogger(l log.Logger) BackgroundCommandOption {
	return func(bg *BackgroundCommand) {
		bg.log = l
	}
}

func BackgroundCommandWithExecutionDurationHistogramCounter(h metrics.Histogram) BackgroundCommandOption {
	return func(bg *BackgroundCommand) {
		bg.executionDurationHistogram = h
	}
}

func (bg *BackgroundCommand) Run() {
	level.Info(bg.log).Log("msg", "running background process")

	t := time.NewTicker(bg.interval)
	defer t.Stop()

	for !bg.isStopped() {
		<-t.C

		bg.executionsCounter.Add(1)
		timer := metrics.NewTimer(bg.executionDurationHistogram)
		timer.Unit(time.Millisecond)

		err := bg.command.Execute(context.Background())
		if err != nil {
			level.Error(bg.log).Log("msg", "command error", "err", err)
		}
		timer.ObserveDuration()
	}

	level.Info(bg.log).Log("msg", "stopping background process")
}

func (c *BackgroundCommand) isStopped() bool {
	return atomic.LoadInt32(&(c.stopped)) != 0
}

func (c *BackgroundCommand) Stop() {
	atomic.StoreInt32(&(c.stopped), int32(1))
}
