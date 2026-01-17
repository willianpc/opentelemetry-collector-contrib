// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"context"
	"fmt"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.38.0"
)

// duckDBExporter is the implementation of file exporter that writes telemetry data to a file
type duckDBExporter struct {
	conf       *Config
	marshaller *marshaller
}

func getServiceName(resAttr pcommon.Map) string {
	if v, ok := resAttr.Get(string(conventions.ServiceNameKey)); ok {
		return v.AsString()
	}

	return ""
}

func (e *duckDBExporter) consumeTraces(_ context.Context, td ptrace.Traces) error {
	_, err := e.marshaller.marshalTraces(td)
	if err != nil {
		return err
	}

	appender, fn, err := withAppender("test.db", "spans")

	if err != nil {
		fmt.Println("FAIL TO ACQUIRE APPEND", err)
	} else {
		defer func() {
			appender.Flush()
			// appender.Close()
			// fmt.Println("appender flushed and closed")
			fn()
		}()
	}

	// fmt.Println("\033[3;36m duckdb :: \033[0m Span count:", td.SpanCount())
	for _, rs := range td.ResourceSpans().All() {

		for _, ss := range rs.ScopeSpans().All() {

			for _, span := range ss.Spans().All() {
				serviceName := getServiceName(rs.Resource().Attributes())
				spanName := span.Name()
				spanId := span.SpanID().String()
				parentId := span.ParentSpanID().String()
				traceId := span.TraceID().String()
				kind := span.Kind().String()
				schemaUrl := rs.SchemaUrl()
				var resourceAttributes = map[string]string{}
				scopeName := ss.Scope().Name()
				scopeVersion := ss.Scope().Version()
				startTimestamp := span.StartTimestamp().AsTime()
				endTimestamp := span.EndTimestamp().AsTime()
				flags := span.Flags()
				statusCode := span.Status().Code().String()
				statusMessage := span.Status().Message()

				for k, v := range rs.Resource().Attributes().All() {
					resourceAttributes[k] = v.AsString()
				}

				var eventTimes []time.Time
				var eventNames []string
				var eventAttrs = []duckdb.Map{}

				for _, ev := range span.Events().All() {
					eventTimes = append(eventTimes, ev.Timestamp().AsTime())
					eventNames = append(eventNames, ev.Name())

					var evAttrs = map[string]string{}

					for k, v := range ev.Attributes().All() {
						evAttrs[k] = v.AsString()
					}

					eventAttrs = append(eventAttrs, duckdbMapFromStringMap(evAttrs))
				}

				var linkTraceIds []string
				var linkSpanIds []string
				var linkTraceStates []string
				var linkAttrs = []duckdb.Map{}

				for _, lnk := range span.Links().All() {
					linkTraceIds = append(linkTraceIds, lnk.TraceID().String())
					linkSpanIds = append(linkSpanIds, lnk.SpanID().String())
					linkTraceStates = append(linkTraceStates, lnk.TraceState().AsRaw())

					var lnkAttr = map[string]string{}
					for k, v := range lnk.Attributes().All() {
						lnkAttr[k] = v.AsString()
					}
					linkAttrs = append(linkAttrs, duckdbMapFromStringMap(lnkAttr))
				}

				if appender != nil {
					err = appender.AppendRow(
						serviceName,
						spanName,
						spanId,
						parentId,
						traceId,
						kind,
						schemaUrl,
						duckdbMapFromStringMap(resourceAttributes),
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
						return err
					}
				}
			}
		}
	}

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
	fmt.Println("duckdb exporter shutdown...")
	return nil
}
