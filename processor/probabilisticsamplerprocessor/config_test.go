// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package probabilisticsamplerprocessor

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/service/servicetest"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id: component.NewIDWithName(typeStr, ""),
			expected: &Config{
				ProcessorSettings:  config.NewProcessorSettings(component.NewID(typeStr)),
				SamplingPercentage: 15.3,
				HashSeed:           22,
				AttributeSource:    "traceID",
			},
		},
		{
			id: component.NewIDWithName(typeStr, "logs"),
			expected: &Config{
				ProcessorSettings:  config.NewProcessorSettings(component.NewIDWithName(typeStr, "logs")),
				SamplingPercentage: 15.3,
				HashSeed:           22,
				AttributeSource:    "record",
				FromAttribute:      "foo",
				SamplingPriority:   "bar",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			require.NoError(t, err)
			processors, err := cm.Sub("processors")
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := processors.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))
			cfg.SetIDName(tt.id.Name())

			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestLoadInvalidConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	require.NoError(t, err)

	factory := NewFactory()
	factories.Processors[typeStr] = factory

	_, err = servicetest.LoadConfigAndValidate(filepath.Join("testdata", "invalid.yaml"), factories)
	require.ErrorContains(t, err, "negative sampling rate: -15.30")
}
