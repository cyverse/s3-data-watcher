package service

import (
	"github.com/cyverse/s3-data-watcher/commons"
	log "github.com/sirupsen/logrus"
)

// S3DataWatcherService is a service object
type S3DataWatcherService struct {
	config *commons.Config

	externalCmdService *ExternalCmdService
	natsService        *NatsService
}

// NewService creates a new Service
func NewService(config *commons.Config) (*S3DataWatcherService, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "Start",
	})

	defer commons.StackTraceFromPanic(logger)

	service := &S3DataWatcherService{
		config: config,
	}

	externalCmdService, err := CreateExternalCmdService(service)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	service.externalCmdService = externalCmdService

	natsService, err := CreateNatsService(service, &config.NatsConfig, externalCmdService.S3EventHandler)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	service.natsService = natsService

	return service, nil
}

// Release releases the service
func (svc *S3DataWatcherService) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "S3DataWatcherService",
		"function": "Release",
	})

	logger.Info("Releasing S3DataWatcherService")

	defer commons.StackTraceFromPanic(logger)

	if svc.natsService != nil {
		svc.natsService.Release()
		svc.natsService = nil
	}

	if svc.externalCmdService != nil {
		svc.externalCmdService.Release()
		svc.externalCmdService = nil
	}
}
