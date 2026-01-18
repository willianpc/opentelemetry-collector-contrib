package internal

import (
	"iter"

	"github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	conventions "go.opentelemetry.io/otel/semconv/v1.38.0"
)

func GetServiceName(resAttr pcommon.Map) string {
	if v, ok := resAttr.Get(string(conventions.ServiceNameKey)); ok {
		return v.AsString()
	}

	return ""
}

func DuckDbMapFromIterable(m iter.Seq2[string, pcommon.Value]) duckdb.Map {
	ddbm := make(duckdb.Map)

	for k, v := range m {
		ddbm[k] = v.AsString()
	}

	return ddbm
}
