package service

import (
	"sync"
	"time"

	"github.com/cyverse/s3-data-watcher/commons"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

type S3EventHandler func([]byte)

type NatsService struct {
	service              *S3DataWatcherService
	config               *commons.NatsConfig
	connection           *nats.Conn
	subscription         *nats.Subscription
	lastConnectTrialTime time.Time
	connectionLock       sync.Mutex
	eventHandler         S3EventHandler
}

// CreateNatsService creates a Nats service object and connects to Nats
func CreateNatsService(service *S3DataWatcherService, config *commons.NatsConfig, hander S3EventHandler) (*NatsService, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateNatsService",
	})

	defer commons.StackTraceFromPanic(logger)

	// lazy connect
	natsService := &NatsService{
		service:              service,
		config:               config,
		lastConnectTrialTime: time.Time{},
		connectionLock:       sync.Mutex{},
		eventHandler:         hander,
	}

	err := natsService.ensureConnected()
	if err != nil {
		logger.WithError(err).Warn("will retry again")
		// ignore error
	}

	return natsService, nil
}

func (natsService *NatsService) ensureConnected() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "NatsService",
		"function": "ensureConnected",
	})

	defer commons.StackTraceFromPanic(logger)

	natsService.connectionLock.Lock()
	defer natsService.connectionLock.Unlock()

	if natsService.connection != nil {
		if natsService.connection.IsClosed() {
			// clear
			natsService.connection = nil
			natsService.subscription = nil
		}
	}

	if natsService.connection == nil || natsService.subscription == nil {
		// disconnected - try to connect
		if time.Now().After(natsService.lastConnectTrialTime.Add(commons.ReconnectInterval)) {
			// passed reconnect interval
			return natsService.connect()
		} else {
			// too early to reconnect
			return NewServiceNotReadyErrorf("ignore reconnect request. will try after %f seconds from last trial", commons.ReconnectInterval.Seconds())
		}
	}

	return nil
}

func (natsService *NatsService) connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "NatsService",
		"function": "connect",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Infof("connecting to Nats %s", natsService.config.URL)

	natsService.lastConnectTrialTime = time.Now()

	natsService.connection = nil

	options := []nats.Option{}

	if natsService.config.MaxReconnects >= 0 {
		options = append(options, nats.MaxReconnects(natsService.config.MaxReconnects))
	}

	if natsService.config.ReconnectWait >= 0 {
		reconnectWait := time.Duration(natsService.config.ReconnectWait) * time.Second
		options = append(options, nats.ReconnectWait(reconnectWait))
	}

	if len(natsService.config.Subject) == 0 {
		err := xerrors.Errorf("failed to subscribe an empty subject")
		logger.Error(err)
		return err
	}

	// Connect to Nats
	connection, err := nats.Connect(natsService.config.URL, options...)
	if err != nil {
		logger.Error(err)
		return err
	}
	natsService.connection = connection

	// Add a handler
	handler := func(msg *nats.Msg) {
		if natsService.eventHandler != nil {
			natsService.eventHandler(msg.Data)
		}
	}

	// use QueueSubscribe API
	subscription, err := connection.Subscribe(natsService.config.Subject, handler)
	if err != nil {
		logger.Error(err)
		connection.Close()
		return err
	}

	natsService.subscription = subscription
	logger.Tracef("established a connection to %s", natsService.config.URL)

	return nil
}

// Release releases all resources, disconnecting from Nats
func (natsService *NatsService) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "NatsService",
		"function": "Release",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Infof("trying to disconnect from %s", natsService.config.URL)

	natsService.connectionLock.Lock()
	defer natsService.connectionLock.Unlock()

	if natsService.subscription != nil {
		natsService.subscription.Unsubscribe()
		natsService.subscription = nil
	}

	if natsService.connection != nil {
		if !natsService.connection.IsClosed() {
			natsService.connection.Close()
		}
		natsService.connection = nil
	}
}
