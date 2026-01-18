// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"
	"fmt"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter/internal"
)

// tracesExporter is the implementation of file exporter that writes telemetry data to a file
type tracesExporter struct {
	conf       *Config
	marshaller *marshaller
	logger     *zap.Logger
}

func (e *tracesExporter) consumeTraces(_ context.Context, td ptrace.Traces) error {
	_, err := e.marshaller.marshalTraces(td)
	if err != nil {
		return err
	}

	appender, closeDbConnections, err := acquireAppenderForTable(e.conf, e.logger, tracesTable)

	if err != nil {
		e.logger.Error(fmt.Sprintf("Failed to acquire appender: %v", err))
		return fmt.Errorf("Failed to acquire appender: %v", err)
	} else {
		defer func() {
			appender.Flush()
			closeDbConnections()
		}()
	}

	for _, rs := range td.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				serviceName := internal.GetServiceName(rs.Resource().Attributes())
				spanName := span.Name()
				spanId := span.SpanID().String()
				parentId := span.ParentSpanID().String()
				traceId := span.TraceID().String()
				kind := span.Kind().String()
				schemaUrl := rs.SchemaUrl()
				resourceAttributes := internal.DuckDbMapFromIterable(rs.Resource().Attributes().All())
				scopeName := ss.Scope().Name()
				scopeVersion := ss.Scope().Version()
				startTimestamp := span.StartTimestamp().AsTime()
				endTimestamp := span.EndTimestamp().AsTime()
				flags := span.Flags()
				statusCode := span.Status().Code().String()
				statusMessage := span.Status().Message()

				var eventTimes []time.Time
				var eventNames []string
				eventAttrs := []duckdb.Map{}

				for _, ev := range span.Events().All() {
					eventTimes = append(eventTimes, ev.Timestamp().AsTime())
					eventNames = append(eventNames, ev.Name())
					evAttrs := internal.DuckDbMapFromIterable(ev.Attributes().All())
					eventAttrs = append(eventAttrs, evAttrs)
				}

				var linkTraceIds []string
				var linkSpanIds []string
				var linkTraceStates []string
				linkAttrs := []duckdb.Map{}

				for _, lnk := range span.Links().All() {
					linkTraceIds = append(linkTraceIds, lnk.TraceID().String())
					linkSpanIds = append(linkSpanIds, lnk.SpanID().String())
					linkTraceStates = append(linkTraceStates, lnk.TraceState().AsRaw())
					lnkAttr := internal.DuckDbMapFromIterable(lnk.Attributes().All())
					linkAttrs = append(linkAttrs, lnkAttr)
				}

				e.logger.Info(fmt.Sprintf("Appending span %s of service %s", spanName, serviceName))

				err = appender.AppendRow(
					serviceName,
					spanName,
					spanId,
					parentId,
					traceId,
					kind,
					schemaUrl,
					resourceAttributes,
					scopeName,
					scopeVersion,
					startTimestamp,
					endTimestamp,
					flags,
					statusCode,
					statusMessage,
					eventTimes,
					eventNames,
					eventAttrs,
					linkTraceIds,
					linkSpanIds,
					linkTraceStates,
					linkAttrs,
				)
				if err != nil {
					e.logger.Error(fmt.Sprintf("Error appending span: %v", err))
					return fmt.Errorf("Error appending span: %v", err)
				}
			}
		}
	}

	return nil
}

// Start starts the flush timer if set.
func (e *tracesExporter) Start(_ context.Context, host component.Host) error {
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
func (e *tracesExporter) Shutdown(context.Context) error {
	// if e.writer == nil {
	// 	return nil
	// }
	// w := e.writer
	// e.writer = nil
	// return w.shutdown()
	return nil
}

func newTracesExporter(logger *zap.Logger, conf *Config) TracesExporter {
	return &tracesExporter{
		conf:   conf,
		logger: logger,
	}
}
