package service

import (
	"github.com/cyverse/s3-data-watcher/commons"
	log "github.com/sirupsen/logrus"
)

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

	logger.Info(string(msg))
}
