package commons

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cyverse/s3-data-watcher/commons"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	ChildProcessArgument = "child_process"
)

func SetCommonFlags(command *cobra.Command) {
	command.Flags().StringP("config", "c", commons.ConfigFilePathDefault, "Set config file (yaml)")
	command.Flags().BoolP("version", "v", false, "Print version")
	command.Flags().BoolP("help", "h", false, "Print help")
	command.Flags().BoolP("debug", "d", false, "Enable debug mode")
	command.Flags().BoolP("foreground", "f", false, "Run in foreground")
	command.Flags().Bool(ChildProcessArgument, false, "")
}

func ProcessCommonFlags(command *cobra.Command) (*commons.Config, io.WriteCloser, bool, error) {
	logger := log.WithFields(log.Fields{
		"package":  "commons",
		"function": "ProcessCommonFlags",
	})

	debug := false
	debugFlag := command.Flags().Lookup("debug")
	if debugFlag != nil {
		debug, _ = strconv.ParseBool(debugFlag.Value.String())
	}

	foreground := false
	foregroundFlag := command.Flags().Lookup("foreground")
	if foregroundFlag != nil {
		foreground, _ = strconv.ParseBool(foregroundFlag.Value.String())
	}

	childProcess := false
	childProcessFlag := command.Flags().Lookup(ChildProcessArgument)
	if childProcessFlag != nil {
		childProcess, _ = strconv.ParseBool(childProcessFlag.Value.String())
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	helpFlag := command.Flags().Lookup("help")
	if helpFlag != nil {
		help, _ := strconv.ParseBool(helpFlag.Value.String())
		if help {
			PrintHelp(command)
			return nil, nil, false, nil // stop here
		}
	}

	versionFlag := command.Flags().Lookup("version")
	if versionFlag != nil {
		version, _ := strconv.ParseBool(versionFlag.Value.String())
		if version {
			PrintVersion(command)
			return nil, nil, false, nil // stop here
		}
	}

	readConfig := false
	var config *commons.Config

	configFlag := command.Flags().Lookup("config")
	if configFlag != nil {
		configPath := configFlag.Value.String()
		if len(configPath) > 0 {
			yamlBytes, err := os.ReadFile(configPath)
			if err != nil {
				logger.Error(err)
				return nil, nil, false, err // stop here
			}

			serverConfig, err := commons.NewConfigFromYAML(yamlBytes)
			if err != nil {
				logger.Error(err)
				return nil, nil, false, err // stop here
			}

			// overwrite config
			config = serverConfig
			readConfig = true
		}
	}

	// default config
	if !readConfig {
		yamlBytes, err := os.ReadFile(commons.ConfigFilePathDefault)
		if err != nil {
			logger.Error(err)
			return nil, nil, false, err // stop here
		}

		serverConfig, err := commons.NewConfigFromYAML(yamlBytes)
		if err != nil {
			logger.Error(err)
			return nil, nil, false, err // stop here
		}

		// overwrite config
		config = serverConfig
		readConfig = true
	}

	if !readConfig {
		config = commons.NewDefaultConfig()
	}

	// prioritize command-line flag over config files
	if debug {
		log.SetLevel(log.DebugLevel)
		config.Debug = true
	}

	if foreground {
		config.Foreground = true
	}

	config.ChildProcess = childProcess

	err := config.MakeLogDir()
	if err != nil {
		logger.Error(err)
		return nil, nil, false, err // stop here
	}

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	var logWriter io.WriteCloser
	logFilePath := config.GetLogFilePath()
	if logFilePath == "-" || len(logFilePath) == 0 {
		log.SetOutput(os.Stderr)
	} else {
		parentLogWriter, parentLogFilePath := getLogWriterForParentProcess(logFilePath)
		logWriter = parentLogWriter

		// use multi output - to output to file and stdout
		mw := io.MultiWriter(os.Stderr, parentLogWriter)
		log.SetOutput(mw)

		logger.Infof("Logging to %s", parentLogFilePath)
	}

	err = config.Validate()
	if err != nil {
		logger.Error(err)
		return nil, logWriter, false, err // stop here
	}

	return config, logWriter, true, nil // continue
}

func PrintVersion(command *cobra.Command) error {
	info, err := commons.GetVersionJSON()
	if err != nil {
		return err
	}

	fmt.Println(info)
	return nil
}

func PrintHelp(command *cobra.Command) error {
	return command.Usage()
}

func getLogWriterForParentProcess(logPath string) (io.WriteCloser, string) {
	logFilePath := fmt.Sprintf("%s.parent", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 5,
		MaxAge:     30, // 30 days
		Compress:   false,
	}, logFilePath
}

func getLogWriterForChildProcess(logPath string) (io.WriteCloser, string) {
	logFilePath := fmt.Sprintf("%s.child", logPath)
	return &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    50, // 50MB
		MaxBackups: 5,
		MaxAge:     30, // 30 days
		Compress:   false,
	}, logFilePath
}
