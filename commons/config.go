package commons

import (
	"os"
	"path"
	"path/filepath"
	"time"

	"golang.org/x/xerrors"
	"gopkg.in/yaml.v2"
)

const (
	ConfigFilePathDefault     string = "/etc/s3_data_watcher/config.yml"
	NatsUrlDefault            string = "nats://nats:4222"
	NatsSubjectDefault        string = ""
	NatsMaxReconnectsDefault  int    = -1
	NatsReconnectWaitDefault  int    = -1
	NatsRequestTimeoutDefault int    = -1

	ReconnectInterval time.Duration = 1 * time.Minute
)

// NatsConfig is a configuration struct for Nats Message bus
type NatsConfig struct {
	URL            string `yaml:"url"`
	Subject        string `yaml:"subject"`
	MaxReconnects  int    `yaml:"max_reconnects,omitempty"`
	ReconnectWait  int    `yaml:"reconnect_wait,omitempty"`
	RequestTimeout int    `yaml:"request_timeout,omitempty"`
}

func getLogFilename() string {
	return "s3_data_watcher.log"
}

func GetDefaultDataRootDirPath() string {
	dirPath, err := os.Getwd()
	if err != nil {
		return "/var/lib/s3_data_watcher"
	}
	return dirPath
}

// Config is a configuration struct
type Config struct {
	DataRootPath string `yaml:"data_root_path,omitempty"`

	// S3 FS Event Publish
	NatsConfig NatsConfig `yaml:"nats_config,omitempty"`

	// for Logging
	LogPath string `yaml:"log_path,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	Debug        bool `yaml:"debug,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`
}

// NewDefaultConfig returns a default config
func NewDefaultConfig() *Config {
	return &Config{
		DataRootPath: GetDefaultDataRootDirPath(),

		NatsConfig: NatsConfig{
			URL:            NatsUrlDefault,
			Subject:        NatsSubjectDefault,
			MaxReconnects:  NatsMaxReconnectsDefault,
			ReconnectWait:  NatsReconnectWaitDefault,
			RequestTimeout: NatsRequestTimeoutDefault,
		},

		LogPath: "", // use default

		Foreground:   false,
		Debug:        false,
		ChildProcess: false,
	}
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(yamlBytes []byte) (*Config, error) {
	config := NewDefaultConfig()

	err := yaml.Unmarshal(yamlBytes, config)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal YAML - %v", err)
	}

	return config, nil
}

// GetLogFilePath returns log file path
func (config *Config) GetLogFilePath() string {
	if len(config.LogPath) > 0 {
		return config.LogPath
	}

	// default
	return path.Join(config.DataRootPath, getLogFilename())
}

// MakeLogDir makes a log dir required
func (config *Config) MakeLogDir() error {
	logFilePath := config.GetLogFilePath()
	logDirPath := filepath.Dir(logFilePath)
	err := config.makeDir(logDirPath)
	if err != nil {
		return err
	}

	return nil
}

func (config *Config) MakeWorkDirs() error {
	err := config.makeDir(config.DataRootPath)
	if err != nil {
		return err
	}

	return nil
}

// makeDir makes a dir for use
func (config *Config) makeDir(path string) error {
	if len(path) == 0 {
		return xerrors.Errorf("failed to create a dir with empty path")
	}

	dirInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(path, 0775)
			if mkdirErr != nil {
				return xerrors.Errorf("making a dir (%s) error - %v", path, mkdirErr)
			}

			return nil
		}

		return xerrors.Errorf("stating a dir (%s) error - %v", path, err)
	}

	if !dirInfo.IsDir() {
		return xerrors.Errorf("a file (%s) exist, not a directory", path)
	}

	dirPerm := dirInfo.Mode().Perm()
	if dirPerm&0200 != 0200 {
		return xerrors.Errorf("a dir (%s) exist, but does not have the write permission", path)
	}

	return nil
}

// Validate validates field values and returns error if occurs
func (config *Config) Validate() error {
	if len(config.DataRootPath) == 0 {
		return xerrors.Errorf("data root dir must be given")
	}

	if len(config.NatsConfig.URL) == 0 {
		return xerrors.Errorf("Nats URL is not given")
	}

	if len(config.NatsConfig.Subject) == 0 {
		return xerrors.Errorf("Nats Subject is not given")
	}

	return nil
}
