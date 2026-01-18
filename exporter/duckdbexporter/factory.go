// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"

	_ "github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter/internal/metadata"
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
	consumeLogs(_ context.Context, ld plog.Logs) error
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
		xexporter.WithLogs(createLogsExporter, metadata.LogsStability),
		// xexporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
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
	ddbe := newTracesExporter(set.Logger, cfg.(*Config))
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
	ddbe := newMetricsExporter(cfg.(*Config))
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
	exporter := newLogsExporter(cfg.(*Config))
	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exporter.consumeLogs,
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}
