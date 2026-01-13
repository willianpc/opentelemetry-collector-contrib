// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

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

type DuckDBExporter interface {
	component.Component
	consumeTraces(_ context.Context, td ptrace.Traces) error
	consumeMetrics(_ context.Context, md pmetric.Metrics) error
	consumeLogs(_ context.Context, ld plog.Logs) error
	consumeProfiles(_ context.Context, pd pprofile.Profiles) error
}

// NewFactory creates a factory for OTLP exporter.
func NewFactory() exporter.Factory {
	return xexporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xexporter.WithTraces(createTracesExporter, metadata.TracesStability),
	// xexporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	// xexporter.WithLogs(createLogsExporter, metadata.LogsStability),
	// xexporter.WithProfiles(createProfilesExporter, metadata.ProfilesStability)
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Enabled: true,
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	ddbe := getOrCreateDuckDBExporter(cfg)
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
	ddbe := getOrCreateDuckDBExporter(cfg)
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
	ddbe := getOrCreateDuckDBExporter(cfg)
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

func createProfilesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (xexporter.Profiles, error) {
	ddbe := getOrCreateDuckDBExporter(cfg)
	return xexporterhelper.NewProfiles(
		ctx,
		set,
		cfg,
		ddbe.consumeProfiles,
		exporterhelper.WithStart(ddbe.Start),
		exporterhelper.WithShutdown(ddbe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

// getOrCreateDuckDBExporter creates a FileExporter and caches it for a particular configuration,
// or returns the already cached one. Caching is required because the factory is asked trace and
// metric receivers separately when it gets CreateTraces() and CreateMetrics()
// but they must not create separate objects, they must use one Exporter object per configuration.
func getOrCreateDuckDBExporter(cfg component.Config) DuckDBExporter {
	conf := cfg.(*Config)
	ddbe := exporters.GetOrAdd(cfg, func() component.Component {
		return newDuckDBExporter(conf)
	})

	c := ddbe.Unwrap()
	return c.(DuckDBExporter)
}

func newDuckDBExporter(conf *Config) DuckDBExporter {
	return &duckDBExporter{
		conf: conf,
	}
}

// This is the map of already created File exporters for particular configurations.
// We maintain this map because the Factory is asked trace and metric receivers separately
// when it gets CreateTraces() and CreateMetrics() but they must not
// create separate objects, they must use one Exporter object per configuration.
var exporters = sharedcomponent.NewSharedComponents()
