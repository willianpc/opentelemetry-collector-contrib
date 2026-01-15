// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package duckdbexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

// Config defines configuration for file exporter.
type Config struct {
	Enabled                    bool  `mapstructure:"enabled"`
	directoryPermissionsParsed int64 `mapstructure:"-"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	return nil
}

// Unmarshal a confmap.Conf into the config struct.
func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return errors.New("empty config for duckdb exporter")
	}
	// first load the config normally
	err := componentParser.Unmarshal(cfg)
	if err != nil {
		return err
	}

	return nil
}
