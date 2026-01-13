// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/duckdbexporter"

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Marshaler configuration used for marshaling Protobuf
var tracesMarshalers = map[string]ptrace.Marshaler{
	formatTypeJSON:  &ptrace.JSONMarshaler{},
	formatTypeProto: &ptrace.ProtoMarshaler{},
}

var metricsMarshalers = map[string]pmetric.Marshaler{
	formatTypeJSON:  &pmetric.JSONMarshaler{},
	formatTypeProto: &pmetric.ProtoMarshaler{},
}

var logsMarshalers = map[string]plog.Marshaler{
	formatTypeJSON:  &plog.JSONMarshaler{},
	formatTypeProto: &plog.ProtoMarshaler{},
}

var profilesMarshalers = map[string]pprofile.Marshaler{
	formatTypeJSON:  &pprofile.JSONMarshaler{},
	formatTypeProto: &pprofile.ProtoMarshaler{},
}

type marshaller struct {
	tracesMarshaler   ptrace.Marshaler
	metricsMarshaler  pmetric.Marshaler
	logsMarshaler     plog.Marshaler
	profilesMarshaler pprofile.Marshaler

	compression string
	compressor  compressFunc

	formatType string
}

func newMarshaller(conf *Config, host component.Host) (*marshaller, error) {
	return &marshaller{
		// formatType:        conf.FormatType,
		tracesMarshaler:   tracesMarshalers[formatTypeJSON],
		metricsMarshaler:  metricsMarshalers[formatTypeJSON],
		logsMarshaler:     logsMarshalers[formatTypeJSON],
		profilesMarshaler: profilesMarshalers[formatTypeJSON],
		compression:       compressionZSTD,
		compressor:        buildCompressor(compressionZSTD),
	}, nil
}

func (m *marshaller) marshalTraces(td ptrace.Traces) ([]byte, error) {
	if m.tracesMarshaler == nil {
		return nil, errors.New("traces are not supported by encoding")
	}
	buf, err := m.tracesMarshaler.MarshalTraces(td)
	if err != nil {
		return nil, err
	}
	// buf = m.compressor(buf)
	return buf, nil
}

func (m *marshaller) marshalMetrics(md pmetric.Metrics) ([]byte, error) {
	if m.metricsMarshaler == nil {
		return nil, errors.New("metrics are not supported by encoding")
	}
	buf, err := m.metricsMarshaler.MarshalMetrics(md)
	if err != nil {
		return nil, err
	}
	buf = m.compressor(buf)
	return buf, nil
}

func (m *marshaller) marshalLogs(ld plog.Logs) ([]byte, error) {
	if m.logsMarshaler == nil {
		return nil, errors.New("logs are not supported by encoding")
	}
	buf, err := m.logsMarshaler.MarshalLogs(ld)
	if err != nil {
		return nil, err
	}
	buf = m.compressor(buf)
	return buf, nil
}

func (m *marshaller) marshalProfiles(pd pprofile.Profiles) ([]byte, error) {
	if m.profilesMarshaler == nil {
		return nil, errors.New("profiles are not supported by encoding")
	}
	buf, err := m.profilesMarshaler.MarshalProfiles(pd)
	if err != nil {
		return nil, err
	}
	buf = m.compressor(buf)
	return buf, nil
}
