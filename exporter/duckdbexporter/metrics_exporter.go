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
	for _, rm := range md.ResourceMetrics().All() {
		resourceMetricsAttr := rm.Resource().Attributes().AsRaw()
		resourceMetricsSchemaUrl := rm.SchemaUrl()

		for _, scope := range rm.ScopeMetrics().All() {
			scopeSchemaUrl := scope.SchemaUrl()
			scopeAttrs := scope.Scope().Attributes().AsRaw()

			for _, metric := range scope.Metrics().All() {
				mDescr := metric.Description()
				mName := metric.Name()
				mUnit := metric.Unit()
				mType := metric.Type().String()

				e.logger.Info(fmt.Sprintf("resMetricAttrs: %v\nresMetricSchemaURL: %s\nscopeSchema: %s\nscopeattrs: %v\nmetric descr: %s\nmetric name: %s\nmetric unit: %s\nmetric type: %s",
					resourceMetricsAttr,
					resourceMetricsSchemaUrl,
					scopeSchemaUrl,
					scopeAttrs,
					mDescr,
					mName,
					mUnit,
					mType),
				)
			}
		}
	}

	return nil
}

func (e *metricsExporter) Start(_ context.Context, host component.Host) error {
	var err error
	e.marshaller, err = newMarshaller(e.conf, host)
	if err != nil {
		return err
	}

	return nil
}

func (e *metricsExporter) Shutdown(context.Context) error {
	return nil
}

func newMetricsExporter(logger *zap.Logger, conf *Config) MetricsExporter {
	return &metricsExporter{
		conf:   conf,
		logger: logger,
	}
}
