// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter/internal"
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
	appender, closeDbConnections, err := acquireAppenderForTable(e.conf, e.logger, logsTable)

	if err != nil {
		e.logger.Error(fmt.Sprintf("Failed to acquire appender: %v", err))
		return fmt.Errorf("Failed to acquire appender: %v", err)
	} else {
		defer func() {
			appender.Flush()
			closeDbConnections()
		}()
	}

	for _, rl := range ld.ResourceLogs().All() {
		resourceUrl := rl.SchemaUrl()

		for _, scope := range rl.ScopeLogs().All() {
			scopeName := scope.Scope().Name()
			scopeVersion := scope.Scope().Version()
			scopeUrl := scope.SchemaUrl()
			scopeAttrs := internal.DuckDbMapFromIterable(scope.Scope().Attributes().All())

			for _, log := range scope.LogRecords().All() {
				flags := uint8(log.Flags())
				logBody := log.Body().AsString()
				logEventName := log.EventName()
				logSpanId := log.SpanID().String()
				logTraceId := log.TraceID().String()
				logSeverityNumber := uint8(log.SeverityNumber())
				logSeverityText := log.SeverityText()
				logTimestamp := log.Timestamp().AsTime()
				serviceName := internal.GetServiceName(rl.Resource().Attributes())

				err = appender.AppendRow(
					logTimestamp,
					logTraceId,
					logSpanId,
					flags,
					logSeverityText,
					logSeverityNumber,
					serviceName,
					logBody,
					resourceUrl,
					internal.DuckDbMapFromIterable(rl.Resource().Attributes().All()),
					scopeUrl,
					scopeName,
					scopeVersion,
					scopeAttrs,
					internal.DuckDbMapFromIterable(log.Attributes().All()),
					logEventName,
				)

				if err != nil {
					e.logger.Error(fmt.Sprintf("Error appending logs: %v", err))
					return fmt.Errorf("Error appending logs: %v", err)
				}

				e.logger.Info("Appending log: " + logBody)
			}
		}
	}

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

func newLogsExporter(logger *zap.Logger, conf *Config) LogsExporter {
	return &logsExporter{
		conf:   conf,
		logger: logger,
	}
}
