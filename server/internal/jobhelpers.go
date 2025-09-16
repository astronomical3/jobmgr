// Helper methods of the JobHandler that developers can use for identifying job continue, error, and success
//   points.  PLEASE DO NOT EDIT.

package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/astronomical3/jobmgr/jobmgrcapnp"
)

// Definition of a helper method of the JobHandler for logging on both the server side, and also sending to
//   the client side, the job error and its log message explaining the job error for a job.
func (j *JobHandler) JobErrorHelper(jobErr error, logMsg string) {
	// Be sure to always release the j.jobCallback 
	// client capability, as the jobFailed() RPC is 
	// a final call.
	defer j.jobCallback.Release()

	j.status = jobmgrcapnp.Status_error
	// Error will trigger an unexpected close of the j.doneChan to abruptly end
	//   the actual running process.
	close(j.doneChan)
	j.endTime = time.Now()
	j.duration = j.endTime.Sub(j.startTime)
	go j.serverLogger.ServerLogError(
		"job",
		j.jobName,
		logMsg,
	)

	// Get a jobFailedPromise that will call the JobFailed() RPC when the .Struct()
	//   is called on it.
	jobFailedPromise, jobFailedRel := j.jobCallback.JobFailed(
		context.Background(),
		func(p jobmgrcapnp.JobCallback_jobFailed_Params) error {
			err := p.SetLogMsg(logMsg)
			if err != nil {
				return err
			}

			p.SetStatus(j.status)

			return err
		},
	)
	defer jobFailedRel()

	// Actually call the jobFailed() RPC to send over the log message and "error" status.
	_, rpcErr := jobFailedPromise.Struct()
	if rpcErr != nil {
		j.serverLogger.ServerLogError(
			"job",
			j.jobName,
			fmt.Sprintf(
				"Job '%s' running process '%s' (job ID %s) failed to report job error via its job callback capability's jobFailed RPC: %v",
				j.jobName,
				j.process,
				j.jobId,
				rpcErr,
			),
		)
	}
}



// Definition of a helper method of the JobHandler for logging on both the server side and client side that the job completed
//   successfully, and setting the job status to "success".
func (j *JobHandler) JobSuccessHelper(logMsg string) {
	// Be sure to always release the j.jobCallback 
	// client capability, as the jobSuccessful() RPC is 
	// a final call.
	defer j.jobCallback.Release()
	
	// Change the status of the job to "success"
	j.status = jobmgrcapnp.Status_success
	// j.doneChan is closed solely to prevent resource leaks.
	close(j.doneChan)
	j.endTime = time.Now()
	j.duration = j.endTime.Sub(j.startTime)
	go j.serverLogger.ServerLogInfo(
		"job",
		j.jobName,
		logMsg,
	)

	// Get a jobSuccessfulPromise that will call the JobFailed() RPC when the .Struct()
	//   is called on it.
	jobSuccessfulPromise, jobSuccessfulRel := j.jobCallback.JobSuccessful(
		context.Background(),
		func(p jobmgrcapnp.JobCallback_jobSuccessful_Params) error {
			err := p.SetLogMsg(logMsg)
			if err != nil {
				return err
			}

			p.SetStatus(j.status)

			return nil
		},
	)
	defer jobSuccessfulRel()

	// Actually call the jobSuccessful() RPC to send over the log message and
	//   "success" status.
	_, rpcErr := jobSuccessfulPromise.Struct()
	if rpcErr != nil {
		j.serverLogger.ServerLogError(
			"job",
			j.jobName,
			fmt.Sprintf(
				"Failed to call job callback capability for job '%s' running process '%s' (job ID %s) to report successful status of process: %v",
				j.jobName,
				j.process,
				j.jobId,
				rpcErr,
			),
		)
	}
}



// Definition of a method of the JobHandler that will be used for trying to update the job's associated
//   callback capability on the client, as well as the server logs, with further progress information 
//   about the job's process being run.
// THIS METHOD DOES NOT CONTAIN A DEFER TO RELEASE THE JOB CALLBACK'S CLIENT CAPABILITY, SO THAT
//   THIS METHOD CAN BE USED REPEATEDLY AT ANY TIME THROUGHOUT THE PROGRESS OF THE JOB'S PROCESS.
func (j *JobHandler) JobUpdateHelper(logMsg string) {
	// Log the logMsg on the server side, then send the same logMsg about the job/process progress to the 
	//   client's associated job callback object for the client to log that info on their
	//   logs.
	go j.serverLogger.ServerLogInfo(
		"job",
		j.jobName,
		logMsg,
	)

	// Get the jobUpdatePromise that will call the UpdateJobLog() RPC when the .Struct()
	//   is called on it.
	jobUpdateLogPromise, jobUpdateLogRel := j.jobCallback.UpdateJobLog(
		context.Background(),
		func(p jobmgrcapnp.JobCallback_updateJobLog_Params) error {
			err := p.SetLogMsg(logMsg)
			if err != nil {
				return err
			}
			return nil
		},
	)
	defer jobUpdateLogRel()

	// Actually call the UpdateJobLog() RPC to send over the job log message.
	_, updateErr := jobUpdateLogPromise.Struct()

	// If the update RPC call returns an error, this will be considered an unexpected job error, 
	//   and the JobErrorHelper() method will be called in order to try and log the 
	//   update process error, and send that log message over to the client.
	// This closes the j.doneChan and changes the job status to "error", so that the 
	//   process listening to that signal will terminate.
	if updateErr != nil {
		j.JobErrorHelper(
			updateErr,
			fmt.Sprintf(
				"Error occurred in updating client job log with latest info using JobCallback_updateJobLog RPC for job '%s' running process '%s' (job ID %s): %v",
				j.jobName,
				j.process,
				j.jobId,
				updateErr,
			),
		)
	}
}