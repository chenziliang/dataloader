package models

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type LogConfig struct {
	Level string `yaml:"level"`
}

func (l *LogConfig) SetDefaults() *LogConfig {
	switch l.Level {
	case "DEBUG":
	case "INFO":
	case "ERROR":
	default:
		l.Level = "INFO"
	}
	return l
}

type CredConfig struct {
	Type string      `yaml:"type"`
	Ctx  interface{} `yaml:"context"`
}

func (cc *CredConfig) Validate() error {
	if cc.Type != "" {
		if cc.Ctx == nil {
			return errors.Errorf("Cred is missing")
		}
	}
	return nil
}

type ServerConfig struct {
	Type      string      `yaml:"type"`
	Addresses []string    `yaml:"addresses"`
	Cred      *CredConfig `yaml:"cred"`
	Ctx       interface{} `yaml:"context"`
}

func (sc *ServerConfig) Validate() error {
	if len(sc.Addresses) == 0 {
		return errors.New("Invalid configuration, servers is not setup correctly")
	}

	if sc.Type == "" {
		return errors.New("Invalid configuration, server type is not setup correctly")
	}

	if sc.Cred != nil && sc.Cred.Validate() != nil {
		return errors.New("Invalid configuration, cred is not setup correctly")
	}

	return nil
}

type Settings struct {
	Concurrency    uint32 `yaml:"concurrency"`
	BatchSize      uint32 `yaml:"batch_size"`
	Interval       uint32 `yaml:"interval"`
	BackFill       uint32 `yaml:"backfill"`
	TotalEntities  uint32 `yaml:"total_entities"`
	LastRunStateDB string `yaml:"last_run_state_db"`
}

func (s *Settings) SetDefaults() *Settings {
	if s.Concurrency == 0 {
		s.Concurrency = 1
	}

	if s.BatchSize == 0 {
		s.BatchSize = 1000
	}

	if s.Interval == 0 {
		s.Interval = 5
	}

	if s.TotalEntities == 0 {
		s.TotalEntities = 1
	}
	return s
}

type Config struct {
	Sink     ServerConfig `yaml:"sink"`
	Settings Settings     `yaml:"settings"`
	Log      LogConfig    `yaml:"log"`
}

func (c *Config) Validate() error {
	if err := c.Sink.Validate(); err != nil {
		return err
	}

	return nil
}

func (c *Config) SetDefaults() *Config {
	c.Settings.SetDefaults()
	c.Log.SetDefaults()
	return c
}

func newConfigFromBytes(data []byte) (*Config, error) {
	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal config data")
	}

	return config, nil
}

func newConfigFromFile(filepath string) (*Config, error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read config file")
	}
	return newConfigFromBytes(data)
}

func newConfig(create func() (*Config, error)) (*Config, error) {
	config, err := create()
	if err != nil {
		return nil, err
	}

	if err = config.Validate(); err != nil {
		return nil, err
	}

	config.SetDefaults()
	return config, nil
}

func NewConfigFromFile(filepath string) (*Config, error) {
	create := func() (*Config, error) {
		return newConfigFromFile(filepath)
	}
	return newConfig(create)
}
