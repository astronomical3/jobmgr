package client

import (
	"fmt"
	"os"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// *************************************************************************************
// Definition of the ClientLoggingObject struct, which different parts of the client
//
//	application (e.g., a ClientRequestObject, a JobCallbackHandler) can send their
//	log messages to, to pass responsibility of logging all client activity --
//	including special log files separate from the client terminal and file logs for
//	each individual submitted job -- to one object.  That way, duplicate log messages
//	for the client activity terminal and file logs are written here, and even all
//	logging for each job the client submits can be performed by this one object, on
//	behalf of the other components of the client application that need to log in an
//	acitivity.
type ClientLoggingObject struct {
	// For logging activity only for a particular submitted job, maps job ID to that
	//   job file logger.
	jobFileLoggerMap map[string]log.Logger
	// For ensuring job log file object dedicated to a particular job ID gets closed
	//   when job is complete.
	jobLogFileMap map[string]os.File
	// For logging all client activity throughout current session on os.Stdout.
	terminalLogger log.Logger
	// For logging all client activity throughout history of client application use.
	clientFileLogger log.Logger
	// Actual file object that logs all client activity history for the
	//   clientFileLogger.  This is closed when client decides to close the
	//   ClientLoggingObject using the .Close() method.
	clientLogFile *os.File
	// Protects access to the jobFileLogger map, in case client app is configured to
	//   submit and track multiple jobs concurrently.
	mu sync.Mutex
}

// Constructor function that creates a terminal logger to log all the activity in the
//
//	current client session, as well as a file for logging all historical client
//	activity, and also a logger for that client history log file.
func NewClientLoggingObject(clientLogFilename string) *ClientLoggingObject {
	terminalLogger := log.NewLogfmtLogger(os.Stdout)
	terminalLogger = log.NewSyncLogger(terminalLogger)
	terminalLogger = level.NewFilter(terminalLogger, level.AllowInfo())
	terminalLogger = log.With(terminalLogger, "time", log.DefaultTimestampUTC)

	// Add filepath to clientlogs directory for the whole job log file path
	clientLogFilename = "jobmgr/client/clientlogs/" + clientLogFilename + ".log"

	// Create the client log file object and its logger
	clientLogFile, err := os.OpenFile(clientLogFilename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}

	clientFileLogger := log.NewLogfmtLogger(clientLogFile)
	clientFileLogger = log.NewSyncLogger(clientFileLogger)
	clientFileLogger = level.NewFilter(clientFileLogger, level.AllowInfo())
	clientFileLogger = log.With(clientFileLogger, "time", log.DefaultTimestampUTC)

	return &ClientLoggingObject{
		jobFileLoggerMap: make(map[string]log.Logger),
		jobLogFileMap:    make(map[string]os.File),
		terminalLogger:   terminalLogger,
		clientFileLogger: clientFileLogger,
		clientLogFile:    clientLogFile,
	}
}

// Method of the ClientLoggingObject that is used for simultaneously logging into the
//
//	terminalLogger and the clientFileLogger activity done on the scope of the entire
//	client application.  This will be on INFO level.
func (clo *ClientLoggingObject) ClientLogInfo(key, value, message string) {
	level.Info(clo.terminalLogger).Log(key, value, "message", message)
	level.Info(clo.clientFileLogger).Log(key, value, "message", message)
}

// Method of the ClientLoggingObject that is used for simultaneously logging into the
//
//	terminalLogger and the clientFileLogger activity done on the scope of the entire
//	client application.  This will be on WARN level.
func (clo *ClientLoggingObject) ClientLogWarn(key, value, message string) {
	level.Warn(clo.terminalLogger).Log(key, value, "message", message)
	level.Warn(clo.clientFileLogger).Log(key, value, "message", message)
}

// Method of the ClientLoggingObject that is used for simultaneously logging into the
//
//	terminalLogger and the clientFileLogger activity done on the scope of the entire
//	client application.  This will be on ERROR level.
func (clo *ClientLoggingObject) ClientLogError(key, value, message string) {
	level.Error(clo.terminalLogger).Log(key, value, "error", message)
	level.Error(clo.clientFileLogger).Log(key, value, "error", message)
}

// Method of the ClientLoggingObject thay is used for closing any resources used by the
//
//	object that need closing before the client session is exited.  Particularly used
//	for closing the clientLogFile that is used for the clientFileLogger object.
func (clo *ClientLoggingObject) Close() {
	clo.clientLogFile.Close()
}

// Method of the ClientLoggingObject that is used for creating a new file to log activity
//
//	for a particular submitted job, and creating a logger to log job activity on the file.
func (clo *ClientLoggingObject) AddJobLog(jobId, jobLogFilename string) {
	// Create job log file for specific submitted job
	jobLogFile, err := os.OpenFile(jobLogFilename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}

	// Create logger to log leveled information on the job log file
	jobFileLogger := log.NewLogfmtLogger(jobLogFile)
	jobFileLogger = level.NewFilter(jobFileLogger, level.AllowInfo())

	// Add job file logger to jobFileLoggerMap, mapped to jobId given by the
	//   JobExecutor service on the server side.
	clo.jobFileLoggerMap[jobId] = jobFileLogger
}

// Method of the ClientLoggingObject that is used for logging INFO-level information
//
//	regarding a job (with a given jobId) on the job file log specific to that jobId.
func (clo *ClientLoggingObject) JobLogInfo(jobId, key, value, message string) {
	clo.mu.Lock()
	defer clo.mu.Unlock()
	jobFileLogger, exists := clo.jobFileLoggerMap[jobId]
	if !exists {
		panic(fmt.Sprintf("method ClientLoggingObject.JobLogInfo: jobFileLogger for job ID %s doesn't exist", jobId))
	}
	level.Info(jobFileLogger).Log(key, value, "message", message)
}

// Method of the ClientLoggingObject that is used for logging WARN-level information
//
//	regarding a job (with a given jobId) on the job file log specific to that jobId.
func (clo *ClientLoggingObject) JobLogWarn(jobId, key, value, message string) {
	clo.mu.Lock()
	defer clo.mu.Unlock()
	jobFileLogger, exists := clo.jobFileLoggerMap[jobId]
	if !exists {
		panic(fmt.Sprintf("method ClientLoggingObject.JobLogWarn: jobFileLogger for job ID %s doesn't exist", jobId))
	}
	level.Warn(jobFileLogger).Log(key, value, "message", message)
}

// Method of the ClientLoggingObject that is used for logging ERROR-level information
//
//	regarding a job (with a given jobId) on the job file log specific to that jobId.
func (clo *ClientLoggingObject) JobLogError(jobId, key, value, message string) {
	clo.mu.Lock()
	defer clo.mu.Unlock()
	jobFileLogger, exists := clo.jobFileLoggerMap[jobId]
	if !exists {
		panic(fmt.Sprintf("method ClientLoggingObject.JobLogError: jobFileLogger for job ID %s doesn't exist", jobId))
	}
	level.Error(jobFileLogger).Log(key, value, "message", message)
}

// Method of the ClientLoggingObject that is used for closing any resources used by the
//
//	specific jobId's job file logger that need closing when job is completed, regardless
//	of if it ends in success or error, or if job is cancelled or timed out.  Particularly used
//	for closing the job log file corresponding to the jobId, remove that file from the
//	jobLogFileMap, and also delete its corresponding job file logger from the jobFileLoggerMap.
func (clo *ClientLoggingObject) JobLogClose(jobId string) {
	delete(clo.jobFileLoggerMap, jobId)

	jobLogFile := clo.jobLogFileMap[jobId]
	jobLogFile.Close()
	delete(clo.jobLogFileMap, jobId)
}
