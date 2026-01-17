// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent"
)

const (

	// the format of encoded telemetry data
	formatTypeJSON  = "json"
	formatTypeProto = "proto"

	// the type of compression codec
	compressionZSTD = "zstd"
)

type TracesExporter interface {
	component.Component
	consumeTraces(_ context.Context, td ptrace.Traces) error
}

type LogsExporter interface {
	component.Component
	consumeLogs(_ context.Context, td plog.Logs) error
}

type MetricsExporter interface {
	component.Component
	consumeMetrics(_ context.Context, td pmetric.Metrics) error
}

// NewFactory creates a factory for OTLP exporter.
func NewFactory() exporter.Factory {
	return xexporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xexporter.WithTraces(createTracesExporter, metadata.TracesStability),
	// xexporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	// xexporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		DatabaseName:    "otel.duckdb",
		TracesTableName: "otel_traces",
		LogsTableName:   "otel_logs",
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	ddbe := getOrCreateTracesExporter(set.Logger, cfg)
	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		ddbe.consumeTraces,
		exporterhelper.WithStart(ddbe.Start),
		exporterhelper.WithShutdown(ddbe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	ddbe := getOrCreateMetricsExporter(cfg)
	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		ddbe.consumeMetrics,
		exporterhelper.WithStart(ddbe.Start),
		exporterhelper.WithShutdown(ddbe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	ddbe := getOrCreateLogsExporter(cfg)
	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		ddbe.consumeLogs,
		exporterhelper.WithStart(ddbe.Start),
		exporterhelper.WithShutdown(ddbe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

// getOrCreateDuckDBExporter creates a FileExporter and caches it for a particular configuration,
// or returns the already cached one. Caching is required because the factory is asked trace and
// metric receivers separately when it gets CreateTraces() and CreateMetrics()
// but they must not create separate objects, they must use one Exporter object per configuration.

func getOrCreateTracesExporter(logger *zap.Logger, cfg component.Config) TracesExporter {
	conf := cfg.(*Config)
	ddbe := exporters.GetOrAdd(cfg, func() component.Component {
		return newTracesExporter(logger, conf)
	})

	c := ddbe.Unwrap()
	return c.(TracesExporter)
}

func getOrCreateLogsExporter(cfg component.Config) LogsExporter {
	conf := cfg.(*Config)
	ddbe := exporters.GetOrAdd(cfg, func() component.Component {
		return newLogsExporter(conf)
	})

	c := ddbe.Unwrap()
	return c.(LogsExporter)
}
func getOrCreateMetricsExporter(cfg component.Config) MetricsExporter {
	conf := cfg.(*Config)
	ddbe := exporters.GetOrAdd(cfg, func() component.Component {
		return newMetricsExporter(conf)
	})

	c := ddbe.Unwrap()
	return c.(MetricsExporter)
}

// This is the map of already created File exporters for particular configurations.
// We maintain this map because the Factory is asked trace and metric receivers separately
// when it gets CreateTraces() and CreateMetrics() but they must not
// create separate objects, they must use one Exporter object per configuration.
var exporters = sharedcomponent.NewSharedComponents()
