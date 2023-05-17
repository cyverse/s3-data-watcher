package service

import "fmt"

// ServiceNotReadyError ...
type ServiceNotReadyError struct {
	message string
}

// NewServiceNotReadyError creates ServiceNotReadyError struct
func NewServiceNotReadyError(message string) *ServiceNotReadyError {
	return &ServiceNotReadyError{
		message: message,
	}
}

// NewServiceNotReadyErrorf creates ServiceNotReadyError struct
func NewServiceNotReadyErrorf(format string, v ...interface{}) *ServiceNotReadyError {
	return &ServiceNotReadyError{
		message: fmt.Sprintf(format, v...),
	}
}

func (e *ServiceNotReadyError) Error() string {
	return e.message
}

// IsServiceNotReadyError evaluates if the given error is ServiceNotReadyError
func IsServiceNotReadyError(err error) bool {
	if _, ok := err.(*ServiceNotReadyError); ok {
		return true
	}

	return false
}
