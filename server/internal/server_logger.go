package internal

import (
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

//**************************************************************************************************
// Definition of the ServerLoggingObject type, which will be used across various parts of the Cap'n
//   Proto Job Manager Service server, from the general server itself, down to the RPCs of the Job
//   Executor and Job Handler (aka Job Query/Cancel) service interfaces of the Job Manager service,
//   as well as the individual processes run by a submitted job (via the Job Handler).
// This object is used for passing responsibility of logging all server activity to one object, and
//   not having to repeat a single activity log message multiple times for both the terminal logger
//   (which logs the history of the current server session) and the persistent server history log 
//   file; instead, this object does that on behalf of whatever part of the Job Manager Service
//   server needs to add in a log message, and manages both the terminal and file logs.
type ServerLoggingObject struct {
	terminalLogger   log.Logger
	serverFileLogger log.Logger
	serverLogFile    *os.File
}

// Constructor function for creating a new ServerLoggingObject
func NewServerLoggingObject(serverLogFilename string) *ServerLoggingObject {
	// Create the server's terminal logger
	terminalLogger := log.NewLogfmtLogger(os.Stdout)
	terminalLogger = log.NewSyncLogger(terminalLogger)
	terminalLogger = level.NewFilter(terminalLogger, level.AllowInfo())
	terminalLogger = log.With(terminalLogger, "time", log.DefaultTimestampUTC)

	// Change directory to serverlogs.
	err := os.Chdir("serverlogs")
	if err != nil {
		panic(err)
	}
	
	// Create the server's persistent log file and its logger
	serverLogFile, err := os.OpenFile(serverLogFilename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}

	serverFileLogger := log.NewLogfmtLogger(serverLogFile)
	serverFileLogger = log.NewSyncLogger(serverFileLogger)
	serverFileLogger = level.NewFilter(serverFileLogger, level.AllowInfo())
	serverFileLogger = log.With(serverFileLogger, "time", log.DefaultTimestampUTC)

	return &ServerLoggingObject{
		terminalLogger: terminalLogger,
		serverFileLogger: serverFileLogger,
		serverLogFile: serverLogFile,
	}
}

// Method of the ServerLoggingObject for logging in an INFO-level message to both the server's
//   terminal log and server's log file.
func (slo *ServerLoggingObject) ServerLogInfo(key, value, message string) {
	level.Info(slo.terminalLogger).Log(key, value, "message", message)
	level.Info(slo.serverFileLogger).Log(key, value, "message", message)
}

// Method of the ServerLoggingObject for logging in a WARN-level message to both the server's
//   terminal log and server's log file.
func (slo *ServerLoggingObject) ServerLogWarn(key, value, message string) {
	level.Warn(slo.terminalLogger).Log(key, value, "message", message)
	level.Warn(slo.serverFileLogger).Log(key, value, "message", message)
}

// Method of the ServerLoggingObject for logging in an ERROR-level message to both the server's
//   terminal log and server's log file.
func (slo *ServerLoggingObject) ServerLogError(key, value, message string) {
	level.Error(slo.terminalLogger).Log(key, value, "error", message)
	level.Error(slo.serverFileLogger).Log(key, value, "error", message)
}

// Method of the ServerLoggingObject for closing any resources used (e.g., the serverLogFile 
//   object used for creating the serverFileLogger for logging important server activity).
func (slo *ServerLoggingObject) Close() {
	slo.serverLogFile.Close()
}