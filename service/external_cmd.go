package service

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"regexp"

	"github.com/aws/aws-lambda-go/events"
	"github.com/cyverse/s3-data-watcher/commons"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"gopkg.in/yaml.v2"
)

// MinIOS3Event which wrap an array of S3EventRecord
type MinIOS3Event struct {
	EventName string                 `json:"EventName"`
	Key       string                 `json:"Key"`
	Records   []events.S3EventRecord `json:"Records"`
}

type Filter struct {
	Events  []string `yaml:"events,omitempty"`
	Buckets []string `yaml:"buckets,omitempty"`
	Objects []string `yaml:"objects,omitempty"`
}

type Job struct {
	Command string `yaml:"command"`
	Filter  Filter `yaml:"filter,omitempty"`
}

type Jobs struct {
	Jobs []Job `yaml:"jobs"`
}

type ExternalCmdService struct {
	service     *S3DataWatcherService
	jobFilePath string
}

// CreateExternalCmdService creates a ExternalCmd service object
func CreateExternalCmdService(service *S3DataWatcherService) (*ExternalCmdService, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateExternalCmdService",
	})

	defer commons.StackTraceFromPanic(logger)

	jobFilePath, err := commons.ExpandHomeDir(service.config.JobFilePath)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	externalCmdService := &ExternalCmdService{
		service:     service,
		jobFilePath: jobFilePath,
	}

	return externalCmdService, nil
}

// Release releases all resources
func (externalCmdService *ExternalCmdService) Release() {
}

func (externalCmdService *ExternalCmdService) S3EventHandler(msg []byte) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "ExternalCmdService",
		"function": "S3EventHandler",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Debug(string(msg))

	s3Event, err := externalCmdService.convToS3Event(msg)
	if err != nil {
		err := xerrors.Errorf("failed to convert message to S3Event")
		logger.Error(err)
		return
	}

	externalCmdService.processEvent(s3Event)
}

func (externalCmdService *ExternalCmdService) convToS3Event(msg []byte) (*events.S3Event, error) {
	var minioS3Event MinIOS3Event
	err := json.Unmarshal(msg, &minioS3Event)
	if err != nil {
		return nil, err
	}

	if len(minioS3Event.EventName) == 0 {
		return nil, xerrors.Errorf("empty event name")
	}

	if len(minioS3Event.Key) == 0 {
		return nil, xerrors.Errorf("empty key")
	}

	return &events.S3Event{
		Records: minioS3Event.Records,
	}, nil
}

func (externalCmdService *ExternalCmdService) readJobFile() (*Jobs, error) {
	jobData, err := os.ReadFile(externalCmdService.jobFilePath)
	if err != nil {
		return nil, err
	}

	var jobs Jobs
	err = yaml.Unmarshal(jobData, &jobs)
	if err != nil {
		return nil, err
	}

	return &jobs, nil
}

func (externalCmdService *ExternalCmdService) processEvent(s3event *events.S3Event) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "ExternalCmdService",
		"function": "processEvent",
	})

	defer commons.StackTraceFromPanic(logger)

	jobs, err := externalCmdService.readJobFile()
	if err != nil {
		err := xerrors.Errorf("failed to read job file")
		logger.Error(err)
		return
	}

	for _, record := range s3event.Records {
		for _, job := range jobs.Jobs {
			accepted := true

			// if no filter is given, just accept
			if len(job.Filter.Events) > 0 {
				pass := false
				for _, eventFilter := range job.Filter.Events {
					if eventFilter == "*" {
						pass = true
						break
					}

					if matched, _ := regexp.MatchString(eventFilter, record.EventName); matched {
						pass = true
						break
					}
				}

				if !pass {
					accepted = false
				}
			}

			if !accepted {
				continue
			}

			if len(job.Filter.Buckets) > 0 {
				pass := false
				for _, bucketFilter := range job.Filter.Buckets {
					if bucketFilter == "*" {
						pass = true
						break
					}

					if matched, _ := regexp.MatchString(bucketFilter, record.S3.Bucket.Name); matched {
						pass = true
						break
					}
				}

				if !pass {
					accepted = false
				}
			}

			if !accepted {
				continue
			}

			if len(job.Filter.Objects) > 0 {
				pass := false
				for _, objectFilter := range job.Filter.Objects {
					if objectFilter == "*" {
						pass = true
						break
					}

					if matched, _ := regexp.MatchString(objectFilter, record.S3.Object.Key); matched {
						pass = true
						break
					}
				}

				if !pass {
					accepted = false
				}
			}

			if !accepted {
				continue
			}

			// run job
			externalCmdService.runJob(&job, record)
		}
	}
}

func (externalCmdService *ExternalCmdService) runJob(job *Job, record events.S3EventRecord) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "ExternalCmdService",
		"function": "runJob",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Infof("running a job - %s", job.Command)

	cmd := exec.Command(job.Command)

	childStdin, err := cmd.StdinPipe()
	if err != nil {
		logger.WithError(err).Error("failed to get job's STDIN")
		return err
	}

	// start
	err = cmd.Start()
	if err != nil {
		logger.WithError(err).Errorf("failed to start a job")
		return err
	}

	// send it to child
	recordJson, err := json.Marshal(record)
	if err != nil {
		logger.Error(err)
		return err
	}

	_, err = io.WriteString(childStdin, string(recordJson))
	if err != nil {
		logger.WithError(err).Error("failed to send via STDIN")
		return err
	}

	childStdin.Close()

	return nil
}
