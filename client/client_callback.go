package client

import (
	"context"
	"fmt"
	"time"

	"github.com/astronomical3/jobmgr/jobmgrcapnp"
)

// ****************************************************************************
// Definition of the JobCallbackHandler struct that is used for creating
//
//	a job-specific log, and logging job-specific activity in that job
//	log file for historical references and for monitoring.
type JobCallbackHandler struct {
	// implements jobmgrcapnp.JobCallbackHandler
	jobName      string
	jobId        string
	process      string
	status       jobmgrcapnp.Status
	clientLogger *ClientLoggingObject
	doneChan     chan struct{}
}

// Constructor function for creating a new JobCallbackHandler that
//
//	eventually will be controlled by the Job Manager service.
func NewJobCallbackHandler(
	jobName string,
	process string,
	clientLogger *ClientLoggingObject,
) JobCallbackHandler {
	return JobCallbackHandler{
		jobName:      jobName,
		process:      process,
		clientLogger: clientLogger,
		doneChan:     make(chan struct{}),
	}
}

// Implementation of the jobmgrcapnp.JobCallback_initialUpdateJobInfo() RPC,
//
//	which uses arguments (namely the "jobid" and "status" parameters) to add
//	the values to populate remaining empty fields of the JobCallbackHandler,
//	and also use the jobId to create a job log file dedicated specifically to
//	a run of the submitted job using the clientLogger.
func (jch JobCallbackHandler) InitialUpdateJobInfo(
	ctx context.Context,
	call jobmgrcapnp.JobCallback_initialUpdateJobInfo,
) error {
	// Obtain all arguments from the server
	args := call.Args()
	jobName, err := args.Jobname()
	if err != nil {
		jch.clientLogger.ClientLogError(
			"rpc",
			"jobmgrcapnp.JobCallback_jobFailed",
			fmt.Sprintf(
				"Failed to receive job name: %v",
				err,
			),
		)
		return err
	}
	jobId, err := args.JobId()
	if err != nil {
		jch.clientLogger.ClientLogError(
			"rpc",
			"jobmgrcapnp.JobCallback_jobFailed",
			fmt.Sprintf(
				"Failed to receive job name: %v",
				err,
			),
		)
		return err
	}
	process, err := args.Process()
	if err != nil {
		jch.clientLogger.ClientLogError(
			"rpc",
			"jobmgrcapnp.JobCallback_jobFailed",
			fmt.Sprintf(
				"Failed to receive job name: %v",
				err,
			),
		)
		return err
	}

	jch.jobName = jobName
	jch.jobId = jobId
	jch.process = process

	// Get the current time and convert it to a readable string format to
	//   string format to include in the job log file name.
	currentTime := time.Now()
	datetimeString := fmt.Sprintf(
		"%d-%d-%d_%d%d",
		currentTime.Year(),
		currentTime.Month(),
		currentTime.Day(),
		currentTime.Hour(),
		currentTime.Minute(),
	)

	// Create the job log file name.
	jobLogFilename := fmt.Sprintf(
		"%s-%s-%s.log",
		jch.jobName,
		jch.jobId,
		datetimeString,
	)

	// Log in the client activity logs that the log file is being created.
	jch.clientLogger.ClientLogInfo(
		"rpc",
		"jobmgrcapnp.JobCallback_initialUpdateJobInfo",
		fmt.Sprintf("Creating new job log file %s", jobLogFilename),
	)

	jch.clientLogger.AddJobLog(jch.jobId, jobLogFilename)
	jch.clientLogger.JobLogInfo(
		jch.jobId,
		"process",
		jch.process,
		fmt.Sprintf(
			"Process has started for job '%s', with jobId '%s'.",
			jch.jobName,
			jch.jobId,
		),
	)
	jch.clientLogger.ClientLogInfo(
		"rpc",
		"jobmgrcapnp.JobCallback_initialUpdateJobInfo",
		fmt.Sprintf(
			"Process '%s' has started for job '%s', with jobId '%s'",
			jch.process,
			jch.jobName,
			jch.jobId,
		),
	)
	return nil
}

// Implementation of the jobmgrcapnp.JobCallback_updateJobLog() RPC, which
//
//	may or may not be used multiple times throighout the execution of the
//	process, and simply sends back a message for the clientLogger to add
//	to the job log file, as well as the overall client activity logs.
func (jch JobCallbackHandler) UpdateJobLog(
	ctx context.Context,
	call jobmgrcapnp.JobCallback_updateJobLog,
) error {
	// Obtain the log message sent from the server
	logMsg, err := call.Args().LogMsg()
	if err != nil {
		jch.clientLogger.JobLogError(
			jch.jobId,
			"process",
			jch.process,
			fmt.Sprintf("Failed to receive log message: %v", err),
		)
		jch.clientLogger.ClientLogError(
			"rpc",
			"jobmgrcapnp.JobCallback_updateJobLog",
			fmt.Sprintf(
				"Failed to receive log message from process '%s': %v",
				jch.process,
				err,
			),
		)
		return err
	}

	// Add the log message to both the job file log and overall client
	//   activity logs.
	jch.clientLogger.JobLogInfo(jch.jobId, "process", jch.process, logMsg)
	jch.clientLogger.ClientLogInfo(
		"rpc",
		"jobmgrcapnp.JobCallback_updateJobLog",
		fmt.Sprintf(
			"Received log message from process '%s' in job '%s' (job ID %s)",
			jch.process,
			jch.jobName,
			jch.jobId,
		),
	)
	jch.clientLogger.ClientLogInfo("jobId", jch.jobId, logMsg)

	return nil
}

// Implementation of the jobmgrcapnp.JobCallback_updateStatus() RPC, which may
//
//	or may not be used multiple times throughout the execution of the process,
//	and simply sends back the Status enum of "exec" (enum 1), in order to ensure
//	the client that the job's requested process has either begun running or is
//	still running.
func (jch JobCallbackHandler) UpdateStatus(
	ctx context.Context,
	call jobmgrcapnp.JobCallback_updateStatus,
) error {
	// Obtain the status enum sent from the server.
	execStatus := call.Args().Status()

	jch.clientLogger.ClientLogInfo(
		"rpc",
		"jobmgrcapnp.JobCallback_updateStatus",
		fmt.Sprintf(
			"Process '%s' in job '%s' (job ID %s) set to 'exec' status",
			jch.process,
			jch.jobName,
			jch.jobId,
		),
	)

	// Add the status to the JobCallbackHandler.status field, and log that the
	//   status is indeed "exec".
	jch.status = execStatus
	jch.clientLogger.JobLogInfo(
		jch.jobId,
		"process",
		jch.process,
		fmt.Sprintf("Process should now be starting, status is: %v", jch.status),	
	)
	return nil
}

// Implementation of the jobmgrcapnp.JobCallback_jobSuccessful() RPC, which
//
//	is called only at the end of the process if it is successful.  Logs that
//	the job was successful, and receives the "success" status (enum 2) to add
//	into the status field of the JobCallbackHandler.
func (jch JobCallbackHandler) JobSuccessful(
	ctx context.Context,
	call jobmgrcapnp.JobCallback_jobSuccessful,
) error {
	// Obtain all arguments from the server
	args := call.Args()
	logMsg, err := args.LogMsg()
	if err != nil {
		jch.clientLogger.JobLogError(
			jch.jobId,
			"process",
			jch.process,
			fmt.Sprintf("Failed to receive log message: %v", err),
		)
		jch.clientLogger.ClientLogError(
			"rpc",
			"jobmgrcapnp.JobCallback_jobSuccessful",
			fmt.Sprintf(
				"Failed to receive log message from process '%s': %v",
				jch.process,
				err,
			),
		)
		return err
	}
	successStatus := args.Status()

	jch.clientLogger.JobLogInfo(jch.jobId, "process", jch.process, logMsg)
	jch.clientLogger.ClientLogInfo(
		"rpc",
		"jobmgrcapnp.JobCallback_jobSuccessful",
		fmt.Sprintf(
			"SUCCESS: Final log message from process '%s' in job %s (job ID %s): %s",
			jch.process,
			jch.jobName,
			jch.jobId,
			logMsg,
		),
	)

	jch.clientLogger.ClientLogInfo(
		"rpc",
		"jobmgrcapnp.JobCallback_jobSuccessful",
		fmt.Sprintf(
			"Setting status of job %s (job ID %s) to 'success'",
			jch.jobName,
			jch.jobId,
		),
	)

	jch.status = successStatus
	jch.clientLogger.JobLogInfo(
		jch.jobId,
		"process",
		jch.process,
		fmt.Sprintf("Process should now have been marked successful, status is: %v", jch.status),		
	)
	close(jch.doneChan)

	return nil
}

// Implementation of the jobmgrcapnp.JobCallback_jobFailed() RPC, which is
//
//	called only if some part of the process failed.  Logs the job error,
//	and receives the "error" status (enum 3) to add into the status field
//	of the JobCallbackHandler.
func (jch JobCallbackHandler) JobFailed(
	ctx context.Context,
	call jobmgrcapnp.JobCallback_jobFailed,
) error {
	// Obtain all arguments from the server
	args := call.Args()
	logMsg, err := args.LogMsg()
	if err != nil {
		jch.clientLogger.JobLogError(
			jch.jobId,
			"process",
			jch.process,
			fmt.Sprintf("Failed to receive log message: %v", err),
		)
		jch.clientLogger.ClientLogError(
			"rpc",
			"jobmgrcapnp.JobCallback_jobFailed",
			fmt.Sprintf(
				"Failed to receive log message from process '%s': %v",
				jch.process,
				err,
			),
		)
		return err
	}
	errorStatus := args.Status()

	jch.clientLogger.JobLogError(
		jch.jobId,
		"process",
		jch.process,
		fmt.Sprintf(
			"FAILED: Job '%s' (job ID %s) ended with error :%s",
			jch.jobName,
			jch.jobId,
			logMsg,
		),
	)
	jch.clientLogger.ClientLogError(
		"rpc",
		"jobmgrcapnp.JobCallback_jobFailed",
		fmt.Sprintf(
			"JOB FAILED: Process '%s' in job '%s' (job ID %s) ended in error, message: %s",
			jch.process,
			jch.jobName,
			jch.jobId,
			logMsg,
		),
	)

	jch.clientLogger.ClientLogInfo(
		"rpc",
		"jobmgrcapnp.JobCallback_jobFailed",
		fmt.Sprintf(
			"Setting status of job '%s' (job ID %s) to 'error'",
			jch.jobName,
			jch.jobId,
		),
	)

	jch.status = errorStatus
	jch.clientLogger.JobLogInfo(
		jch.jobId,
		"process",
		jch.process,
		fmt.Sprintf("Process should now have been marked as failed, status is: %v", jch.status),	
	)
	close(jch.doneChan)

	return nil
}

// Implementation of the jobmgrcapnp.JobCallback_jobCancelled() RPC, which
//
//	is only called if the client decides to cancel the RPC, or the timeout
//	context set by the client is timed out.  Logs that the job was cancelled
//	by the client, and sets the status field in the JobCallbackHandler to
//	"cancel" (enum 4).
func (jch JobCallbackHandler) JobCancelled(
	ctx context.Context,
	call jobmgrcapnp.JobCallback_jobCancelled,
) error {
	// Obtain all arguments from the server
	args := call.Args()
	logMsg, err := args.LogMsg()
	if err != nil {
		jch.clientLogger.JobLogError(
			jch.jobId,
			"process",
			jch.process,
			fmt.Sprintf("Failed to receive log message: %v", err),
		)
		jch.clientLogger.ClientLogError(
			"rpc",
			"jobmgrcapnp.JobCallback_jobCancelled",
			fmt.Sprintf(
				"Failed to receive log message from process '%s': %v",
				jch.process,
				err,
			),
		)
		return err
	}
	cancelStatus := args.Status()

	jch.clientLogger.JobLogWarn(
		jch.jobId,
		"process",
		jch.process,
		fmt.Sprintf(
			"CANCELLATION WARNING: Job '%s' (job ID %s) cancelled: %s",
			jch.jobName,
			jch.jobId,
			logMsg,
		),
	)
	jch.clientLogger.ClientLogWarn(
		"rpc",
		"jobmgrcapnp.JobCallback_jobCancelled",
		fmt.Sprintf(
			"JOB CANCELLATION WARNING: Process '%s' in job '%s' (job ID %s) was cancelled, cause was: %s",
			jch.process,
			jch.jobName,
			jch.jobId,
			logMsg,
		),
	)

	jch.clientLogger.ClientLogInfo(
		"rpc",
		"jobmgrcapnp.JobCallback_jobFailed",
		fmt.Sprintf(
			"Setting status of job '%s' (job ID %s) to 'cancel'",
			jch.jobName,
			jch.jobId,
		),
	)

	jch.status = cancelStatus
	jch.clientLogger.JobLogInfo(
		jch.jobId,
		"process",
		jch.process,
		fmt.Sprintf("Process should now have been marked as cancelled, status is: %v", jch.status),	
	)
	close(jch.doneChan)

	return nil
}
