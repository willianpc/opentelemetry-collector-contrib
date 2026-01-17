// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type metricsExporter struct {
	conf       *Config
	marshaller *marshaller
	logger     *zap.Logger
}

func (e *metricsExporter) consumeMetrics(_ context.Context, md pmetric.Metrics) error {
	// buf, err := e.marshaller.marshalMetrics(md)
	// if err != nil {
	// 	return err
	// }
	// return e.writer.export(buf)
	return nil
}

// Start starts the flush timer if set.
func (e *metricsExporter) Start(_ context.Context, host component.Host) error {
	var err error
	e.marshaller, err = newMarshaller(e.conf, host)
	if err != nil {
		return err
	}
	// export := buildExportFunc(e.conf)

	// Optionally ensure the output directory exists.
	// if e.conf.CreateDirectory {
	// 	dir := filepath.Dir(e.conf.Path)
	// 	perm := os.FileMode(0o755)
	// 	if e.conf.directoryPermissionsParsed != 0 {
	// 		perm = os.FileMode(e.conf.directoryPermissionsParsed)
	// 	}
	// 	err = os.MkdirAll(dir, perm)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// e.writer, err = newFileWriter(e.conf.Path, e.conf.Append, e.conf.Rotation, e.conf.FlushInterval, export)
	// if err != nil {
	// 	return err
	// }
	// e.writer.start()
	return nil
}

// Shutdown stops the exporter and is invoked during shutdown.
// It stops the flush ticker if set.
func (e *metricsExporter) Shutdown(context.Context) error {
	// if e.writer == nil {
	// 	return nil
	// }
	// w := e.writer
	// e.writer = nil
	// return w.shutdown()
	fmt.Println("duckdb exporter shutdown...")
	return nil
}

func newMetricsExporter(conf *Config) MetricsExporter {
	return &metricsExporter{
		conf: conf,
	}
}
