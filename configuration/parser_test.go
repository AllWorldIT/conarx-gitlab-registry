package configuration

import (
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type localConfiguration struct {
	Version Version `yaml:"version"`
	Log     *Log    `yaml:"log"`
}

type Log struct {
	Formatter string `yaml:"formatter,omitempty"`
}

var expectedConfig = localConfiguration{
	Version: "0.1",
	Log: &Log{
		Formatter: "json",
	},
}

func TestParserSuite(t *testing.T) {
	suite.Run(t, new(ParserSuite))
}

type ParserSuite struct {
	suite.Suite
}

func (s *ParserSuite) TestParserOverwriteInitializedPointer() {
	config := localConfiguration{}

	err := os.Setenv("REGISTRY_LOG_FORMATTER", "json")
	require.NoError(s.T(), err)
	defer os.Unsetenv("REGISTRY_LOG_FORMATTER")

	p := NewParser("registry", []VersionedParseInfo{
		{
			Version: "0.1",
			ParseAs: reflect.TypeOf(config),
			ConversionFunc: func(c any) (any, error) {
				return c, nil
			},
		},
	})

	err = p.Parse([]byte(`{version: "0.1", log: {formatter: "text"}}`), &config)
	require.NoError(s.T(), err)
	require.Equal(s.T(), expectedConfig, config)
}

func (s *ParserSuite) TestParseOverwriteUninitializedPointer() {
	config := localConfiguration{}

	err := os.Setenv("REGISTRY_LOG_FORMATTER", "json")
	require.NoError(s.T(), err)
	defer os.Unsetenv("REGISTRY_LOG_FORMATTER")

	p := NewParser("registry", []VersionedParseInfo{
		{
			Version: "0.1",
			ParseAs: reflect.TypeOf(config),
			ConversionFunc: func(c any) (any, error) {
				return c, nil
			},
		},
	})

	err = p.Parse([]byte(`{version: "0.1"}`), &config)
	require.NoError(s.T(), err)
	require.Equal(s.T(), expectedConfig, config)
}

func (s *ParserSuite) TestOverwriteStructInline() {
	tests := []struct {
		name   string
		config any
	}{
		{
			name: "inline alone",
			config: &struct {
				Redis struct {
					RedisCommon `yaml:",inline"`
				} `yaml:"redis"`
			}{},
		},
		{
			name: "inline before other",
			config: &struct {
				Redis struct {
					RedisCommon `yaml:",inline,omitempty"`
				} `yaml:"redis"`
			}{},
		},
		{
			name: "inline after other",
			config: &struct {
				Redis struct {
					RedisCommon `yaml:",omitempty,inline"`
				} `yaml:"redis"`
			}{},
		},
	}

	err := os.Setenv("REGISTRY_REDIS_ADDR", "5.6.7.8:6379")
	require.NoError(s.T(), err)
	defer os.Unsetenv("REGISTRY_REDIS_ADDR")

	for _, tt := range tests {
		s.Run(tt.name, func() {
			config := tt.config
			p := NewParser("registry", []VersionedParseInfo{
				{
					Version:        "0.1",
					ParseAs:        reflect.TypeOf(config).Elem(),
					ConversionFunc: func(c any) (any, error) { return c, nil },
				},
			})

			v := reflect.ValueOf(config).Elem()
			err = p.overwriteStruct(v, "REGISTRY_REDIS_ADDR", []string{"REDIS", "ADDR"}, "5.6.7.8:6379")
			require.NoError(s.T(), err)

			got := v.FieldByName("Redis").FieldByName("RedisCommon").FieldByName("Addr").String()
			require.Equal(s.T(), "5.6.7.8:6379", got)
		})
	}
}
