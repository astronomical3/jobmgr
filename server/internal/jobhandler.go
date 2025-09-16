// Handler that represents an individual submitted job, and also implements the jobmgrcapnp.JobQueryCancel
//   so that a client that submitted the job can control the job by either querying/monitoring or cancelling
//   the submitted job.  PLEASE DO NOT EDIT.

package internal

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/astronomical3/jobmgr/jobmgrcapnp"
)

//*************************************************************************************************
// Struct that represents a single, individual submitted job.  Also implements RPCs for the 
//   jobmgrcapnp.JobCancelQuery service interface, so that clients that have the client capability
//   to this interface can query/monitor and (if necessary) cancel the job by controlling the job
//   struct and its actual running "process" here.
// While processes are actually methods to this structure, they will be defined in a separate 
//   "internal" package, so as to make the process of writing and registering custom processes
//   easier and more organized.
// Here, only the methods implementing the RPCs for the JobCancelQuery capability will be defined
//   in the "server" package.
type JobHandler struct {
	// Initial values upon creation of the individual job, passed from the
	//   client to the Job Executor service object, down to this individual
	//   job.
	jobName        string
	jobId          string
	process        string
	serverLogger   *ServerLoggingObject
	jobCallback    jobmgrcapnp.JobCallback

	// Values and objects that will be received and/or used by the job later
	//   on throughout the actual job submission and process execution 
	//   procedures.
	status         jobmgrcapnp.Status
	startTime      time.Time
	endTime        time.Time
	duration       time.Duration
	isCancelled    atomic.Bool
	isActive       atomic.Bool
	doneChan       chan struct{}
}

// Constructor function for creating a new JobHandler.
func NewJobHandler(jobName, jobId, process string, jobCallback jobmgrcapnp.JobCallback, serverLogger *ServerLoggingObject) *JobHandler {
	newJob := &JobHandler{
		jobName: jobName,
		jobId: string(jobId),
		process: process,
		jobCallback: jobCallback,
		status: jobmgrcapnp.Status_submit,
		doneChan: make(chan struct{}),
		serverLogger: serverLogger,
	}

	// Set the isCancelled atomic boolean to "false" as default value
	newJob.isCancelled.Store(false)

	// Set the isActive atomic boolean to "true" as default value
	newJob.isActive.Store(false)

	return newJob
}

// Implementation of the jobmgrcapnp.JobQueryCancel_queryJob() RPC.  This is called by the 
//   Job Manager Service client application's core ClientRequestObject (representing the 
//   specific job the client has submitted), so that the client can check that the submitted
//   job is still running or not.
func (j JobHandler) QueryJob(ctx context.Context, call jobmgrcapnp.JobQueryCancel_queryJob) error {
	// Create a jobIsActive boolean variable, and set it to true.
	select {
	case <-j.doneChan:
		// Force the isActive bool to "false"
		j.isActive.Store(false)
	default:
		// The jobIsActive boolean remains true.
	}

	// Allocate space for fulfilling the promise of the value indicating 
	//   whether or not the job is still active.
	results, err := call.AllocResults()
	if err != nil {
		j.serverLogger.ServerLogError(
			"job",
			j.jobName,
			fmt.Sprintf(
				"Error in JobHandler_queryJob RPC, in calling call.AllocResults: %v",
				err,
			),
		)
		return err
	}

	// Fulfill the promise of the value indicating whether or not the job
	//   is still active.
	results.SetActive(j.isActive.Load())
	return nil
}

// Implementation of the jobmgrcapnp.JobQueryCancel_cancelJob() RPC.  This is called by
//   the ClientRequestObject (representing the specific job the client app has submitted),
//   so that the client can request that the specific job be cancelled (i.e., if their
//   local timeout has reached or exceeded).
func (j JobHandler) CancelJob(ctx context.Context, call jobmgrcapnp.JobQueryCancel_cancelJob) error {
	// Always be sure, when this RPC is done, that the client capability to the
	//   client application's job callback will be released, as this is a final RPC
	//   call.
	defer j.jobCallback.Release()
	// Check if the j.isCancelled boolean is still false, and if so, change to true.
	j.isCancelled.CompareAndSwap(false, true)
	// Change job status to cancel
	j.status = jobmgrcapnp.Status_cancel
	// Send a close signal to the j.doneChan channel, forcing the job/process to be
	//   cancelled abruptly.
	close(j.doneChan)

	// Get the end time and duration
	j.endTime = time.Now()
	j.duration = j.endTime.Sub(j.startTime)

	// Send out a call to the job's associated jobmgrcapnp.JobCallback client capability's
	//   jobCancelled() RPC.
	jobCancelledPromise, jobCancelledRel := j.jobCallback.JobCancelled(
		context.Background(),
		func(p jobmgrcapnp.JobCallback_jobCancelled_Params) error {
			err := p.SetLogMsg(
				fmt.Sprintf(
					"Job '%s' running process '%s' (job ID %s) has been cancelled by the client.",
					j.jobName,
					j.process,
					j.jobId,
				),
			)
			if err != nil {
				return err
			}

			p.SetStatus(j.status)

			return nil
		},
	)
	defer jobCancelledRel()

	// Call on the jobCancelledPromise to actually send over the parameters and perform
	//   the RPC (that is, log the sent over log message into their client logs and job log)
	_, err := jobCancelledPromise.Struct()
	if err != nil {
		j.serverLogger.ServerLogError(
			"job",
			j.jobName,
			fmt.Sprintf(
				"Failure in cancelling job '%s' (job ID %s) through job callback capability's jobCancelled() RPC: %v",
				j.jobName,
				j.jobId,
				err,
			),
		)
		return err
	}

	// Allocate space for fulfilling the promise of returning the value indicating
	//   whether or not the job has been cancelled.
	isCancelledBoolResults, err := call.AllocResults()
	if err != nil {
		j.serverLogger.ServerLogError(
			"job",
			"j.jobName",
			fmt.Sprintf(
				"Error in JobQueryCancel_cancelJob RPC, in calling call.AllocResults: %v",
				err,
			),
		)
		return err
	}
	
	// Fulfill the promise of the value indicating whether or not the job status has been able
	//   to turn to cancelled.
	isCancelledBoolResults.SetCancelled(j.isCancelled.Load())
	return nil
}