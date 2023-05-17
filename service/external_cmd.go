package service

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/cyverse/s3-data-watcher/commons"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// MinIOS3Event which wrap an array of S3EventRecord
type MinIOS3Event struct {
	EventName string                 `json:"EventName"`
	Key       string                 `json:"Key"`
	Records   []events.S3EventRecord `json:"Records"`
}

type ExternalCmdService struct {
	service *S3DataWatcherService
}

// CreateExternalCmdService creates a ExternalCmd service object
func CreateExternalCmdService(service *S3DataWatcherService) (*ExternalCmdService, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateExternalCmdService",
	})

	defer commons.StackTraceFromPanic(logger)

	externalCmdService := &ExternalCmdService{
		service: service,
	}

	return externalCmdService, nil
}

// Release releases all resources
func (externalCmdService *ExternalCmdService) Release() {
}

func (externalCmdService *ExternalCmdService) S3EventHandler(msg []byte) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
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

	for _, record := range s3Event.Records {
		logger.Infof("eventName: %s, bucket: %s, object: %s", record.EventName, record.S3.Bucket.Name, record.S3.Object.Key)
	}
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
