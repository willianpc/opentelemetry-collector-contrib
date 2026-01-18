// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type logsExporter struct {
	conf       *Config
	marshaller *marshaller
	logger     *zap.Logger
}

func (e *logsExporter) consumeLogs(_ context.Context, ld plog.Logs) error {

	for _, rl := range ld.ResourceLogs().All() {
		resourceAttrs := rl.Resource().Attributes().AsRaw()
		fmt.Println("resource log attrs", resourceAttrs)

		for _, scope := range rl.ScopeLogs().All() {
			scopeName := scope.Scope().Name()
			scopeVersion := scope.Scope().Version()

			fmt.Println("scope name and version:", scopeName, scopeVersion)

			for _, log := range scope.LogRecords().All() {
				flags := log.Flags()
				logAttrs := log.Attributes().AsRaw()
				logBody := log.Body().AsString()
				logEventName := log.EventName()
				logObsTimestamp := log.ObservedTimestamp().AsTime()
				logSpanId := log.SpanID().String()
				logTraceId := log.TraceID().String()
				logSeverityNumber := log.SeverityNumber().String()
				logSeverityText := log.SeverityText()
				logTimestamp := log.Timestamp().AsTime()

				fmt.Printf("log flags: %d\n, attrs: %v\n, body: %s\n, event name: %s\n, observed timestamp: %v\n, span id: %s\n, trace id: %s\n, sev number: %s\n, sev text: %s\n, timestamp: %v\n",
					flags, logAttrs, logBody, logEventName, logObsTimestamp, logSpanId, logTraceId, logSeverityNumber, logSeverityText, logTimestamp,
				)
			}
		}
	}

	// buf, err := e.marshaller.marshalMetrics(md)
	// if err != nil {
	// 	return err
	// }
	// return e.writer.export(buf)
	return nil
}

// Start starts the flush timer if set.
func (e *logsExporter) Start(_ context.Context, host component.Host) error {
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
func (e *logsExporter) Shutdown(context.Context) error {
	// if e.writer == nil {
	// 	return nil
	// }
	// w := e.writer
	// e.writer = nil
	// return w.shutdown()
	fmt.Println("duckdb exporter shutdown...")
	return nil
}

func newLogsExporter(conf *Config) LogsExporter {
	return &logsExporter{
		conf: conf,
	}
}
