package main

import (
	"github.com/rcrowley/go-metrics"
	"github.com/rs/xid"
	"github.com/vrischmann/go-metrics-influxdb"
	"time"
)

type Metrics struct {
	Running     metrics.Gauge
	Error       metrics.Gauge
	ExpireCount metrics.Counter
	Duration    metrics.Gauge
}

func (m *Metrics) startMetrics(r metrics.Registry, config Config, params PARAM) {
	go influxdb.InfluxDBWithTags(r, 5e9,
		config.MetricsConfig.Host,
		config.MetricsConfig.Database,
		config.MetricsConfig.Username,
		config.MetricsConfig.Password,
		map[string]string{"env": config.Environment, "job": params.operation, "query": params.queryId, "id": xid.New().String(), "app": "downsampling.controller"},
	)
}

func NewMetrics(config Config, param PARAM) *Metrics {
	var m Metrics
	r := metrics.NewRegistry()
	m.Duration = metrics.NewGauge()
	r.Register("downsampling.controller.duration", m.Duration)
	m.Running = metrics.NewGauge()
	r.Register("downsampling.controller.running", m.Running)
	m.Error = metrics.NewGauge()
	r.Register("downsampling.controller.error", m.Error)
	m.ExpireCount = metrics.NewCounter()
	r.Register("downsampling.controller.expiredCount", m.ExpireCount)

	metrics.RegisterDebugGCStats(r)
	go metrics.CaptureDebugGCStats(r, 5e9)

	metrics.RegisterRuntimeMemStats(r)
	go metrics.CaptureRuntimeMemStats(r, 5e9)

	m.startMetrics(r, config, param)
	return &m
}

func (m *Metrics) SetDuration(started int64) {
	m.Duration.Update(time.Now().Unix() - started)
}

func (m *Metrics) IncrementExpiredCount() {
	m.ExpireCount.Inc(1)
}

func (m *Metrics) ReportError() {
	m.Error.Update(1)
}

func (m *Metrics) SetRunning() {
	m.Running.Update(1)
}
