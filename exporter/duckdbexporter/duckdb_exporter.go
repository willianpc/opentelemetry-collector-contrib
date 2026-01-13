// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// duckDBExporter is the implementation of file exporter that writes telemetry data to a file
type duckDBExporter struct {
	conf       *Config
	marshaller *marshaller
}

func (e *duckDBExporter) consumeTraces(_ context.Context, td ptrace.Traces) error {
	buf, err := e.marshaller.marshalTraces(td)
	if err != nil {
		return err
	}

	testDuckdb()

	fmt.Println("\033[3;36m duckdb :: \033[0m", string(buf))
	return nil
}

func (e *duckDBExporter) consumeMetrics(_ context.Context, md pmetric.Metrics) error {
	// buf, err := e.marshaller.marshalMetrics(md)
	// if err != nil {
	// 	return err
	// }
	// return e.writer.export(buf)
	return nil
}

func (e *duckDBExporter) consumeLogs(_ context.Context, ld plog.Logs) error {
	// buf, err := e.marshaller.marshalLogs(ld)
	// if err != nil {
	// 	return err
	// }
	// return e.writer.export(buf)
	return nil
}

func (e *duckDBExporter) consumeProfiles(_ context.Context, pd pprofile.Profiles) error {
	// buf, err := e.marshaller.marshalProfiles(pd)
	// if err != nil {
	// 	return err
	// }
	// return e.writer.export(buf)
	return nil
}

// Start starts the flush timer if set.
func (e *duckDBExporter) Start(_ context.Context, host component.Host) error {
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
func (e *duckDBExporter) Shutdown(context.Context) error {
	// if e.writer == nil {
	// 	return nil
	// }
	// w := e.writer
	// e.writer = nil
	// return w.shutdown()
	return nil
}
