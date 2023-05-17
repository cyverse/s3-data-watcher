package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	cmd_commons "github.com/cyverse/s3-data-watcher/cmd/commons"
	"github.com/cyverse/s3-data-watcher/commons"
	"github.com/cyverse/s3-data-watcher/service"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "s3-data-watcher [args..]",
	Short: "Run s3-data-watcher",
	Long:  "Run s3-data-watcher.",
	RunE:  processCommand,
}

func Execute() error {
	return rootCmd.Execute()
}

func processCommand(command *cobra.Command, args []string) error {
	// check if this is subprocess running in the background
	isChildProc := false
	childProcessArgument := fmt.Sprintf("-%s", cmd_commons.ChildProcessArgument)

	for _, arg := range os.Args {
		if len(arg) >= len(childProcessArgument) {
			if arg == childProcessArgument || arg[1:] == childProcessArgument {
				// background
				isChildProc = true
				break
			}
		}
	}

	if isChildProc {
		// child process
		childMain(command, args)
	} else {
		// parent process
		parentMain(command, args)
	}

	return nil
}

func main() {
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000000",
		FullTimestamp:   true,
	})

	log.SetLevel(log.InfoLevel)

	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	// attach common flags
	cmd_commons.SetCommonFlags(rootCmd)

	err := Execute()
	if err != nil {
		logger.Fatal(err)
		os.Exit(1)
	}
}

// parentMain handles command-line parameters and run parent process
func parentMain(command *cobra.Command, args []string) {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "parentMain",
	})

	config, logWriter, cont, err := cmd_commons.ProcessCommonFlags(command)
	if logWriter != nil {
		defer logWriter.Close()
	}

	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}

	if !cont {
		os.Exit(0)
	}

	if !config.Foreground {
		// background
		childStdin, childStdout, err := cmd_commons.RunChildProcess(os.Args[0])
		if err != nil {
			logger.WithError(err).Error("failed to run s3-data-watcher child process")
			os.Exit(1)
		}

		err = cmd_commons.ParentProcessSendConfigViaSTDIN(config, childStdin, childStdout)
		if err != nil {
			logger.WithError(err).Error("failed to send configuration to s3-data-watcher child process")
			os.Exit(1)
		}
	} else {
		// run foreground
		err = run(config, false)
		if err != nil {
			logger.WithError(err).Error("failed to run s3-data-watcher")
			os.Exit(1)
		}
	}
}

// childMain runs child process
func childMain(command *cobra.Command, args []string) {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "childMain",
	})

	logger.Info("Start child process")

	// read from stdin
	config, logWriter, err := cmd_commons.ChildProcessReadConfigViaSTDIN()
	if logWriter != nil {
		defer logWriter.Close()
	}

	if err != nil {
		logger.WithError(err).Error("failed to communicate to parent process")
		cmd_commons.ReportChildProcessError()
		os.Exit(1)
	}

	config.ChildProcess = true

	logger.Info("Run child process")

	// background
	err = run(config, true)
	if err != nil {
		logger.WithError(err).Error("failed to run s3-data-watcher")
		os.Exit(1)
	}

	if logWriter != nil {
		logWriter.Close()
	}
}

// run runs s3-data-watcher
func run(config *commons.Config, isChildProcess bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "run",
	})

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	versionInfo := commons.GetVersion()
	logger.Infof("s3-data-watcher version - %s, commit - %s", versionInfo.ReleaseVersion, versionInfo.GitCommit)

	// make work dirs required
	err := config.MakeWorkDirs()
	if err != nil {
		logger.WithError(err).Error("invalid configuration")
		return err
	}

	err = config.Validate()
	if err != nil {
		logger.WithError(err).Error("invalid configuration")
		return err
	}

	// run a service
	svc, err := service.NewService(config)
	if err != nil {
		logger.WithError(err).Error("failed to create the service")
		if isChildProcess {
			cmd_commons.ReportChildProcessError()
		}
		return err
	}

	if isChildProcess {
		cmd_commons.ReportChildProcessStartSuccessfully()
		if len(config.GetLogFilePath()) == 0 {
			cmd_commons.SetNilLogWriter()
		}
	}

	defer svc.Release()

	// wait
	waitForCtrlC()

	return nil
}

func waitForCtrlC() {
	var endWaiter sync.WaitGroup

	endWaiter.Add(1)
	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, os.Interrupt)

	go func() {
		<-signalChannel
		endWaiter.Done()
	}()

	endWaiter.Wait()
}
