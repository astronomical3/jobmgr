package client

import (
	"context"
	"fmt"
	"net"
	"time"

	"capnproto.org/go/capnp/v3/rpc"
	"github.com/astronomical3/jobmgr/jobmgrcapnp"
)

// ****************************************************************************************
// Definition of the ClientRequestObject that will actually submit the requested job into
//
//	the Job Manager service (aka its Job Executor service), and query and/or cancel the
//	job.
type ClientRequestObject struct {
	address            string
	port               string
	timeout            int
	regConn            net.Conn
	rpcConn            *rpc.Conn
	jobSubmission      jobmgrcapnp.JobSubmitInfo
	jobName            string
	process            string
	jobMgrCap          jobmgrcapnp.JobExecutor
	jobQueryCancel     jobmgrcapnp.JobQueryCancel
	jobCallbackHandler JobCallbackHandler
	jobCallbackCap     jobmgrcapnp.JobCallback
	clientLogger       *ClientLoggingObject
}

// Constructor function for creating a new ClientRequestObject.
func NewRequest(jobSubmission jobmgrcapnp.JobSubmitInfo, address, port string, timeout int, clientLogger *ClientLoggingObject) *ClientRequestObject {
	return &ClientRequestObject{
		jobSubmission: jobSubmission,
		address:       address,
		port:          port,
		timeout:       timeout,
		clientLogger:  clientLogger,
	}
}

// Connects to the server, and also creates a JobCallbackHandler and a jobmgrcapnp.JobCallback
//
//	capability to return when it submits the job.
func (req *ClientRequestObject) ConnectToServer() error {
	req.jobName, _ = req.jobSubmission.JobName()
	req.process, _ = req.jobSubmission.Process()
	req.clientLogger.ClientLogInfo(
		"object",
		"ClientRequestObject",
		fmt.Sprintf(
			"Client request stub created for job '%s', which will run process '%s'. Now dialing up to Cap'n Proto Job Manager server",
			req.jobName,
			req.process,
		),
	)

	max_tries := 3
	var err error
	for i := 0; i < max_tries; i++ {
		req.regConn, err = net.Dial("tcp", net.JoinHostPort(req.address, req.port))
		if err != nil {
			req.clientLogger.ClientLogError(
				"method",
				"ClientRequestObject.ConnectToServer",
				fmt.Sprintf(
					"Error in dialing to Cap'n Proto Job Manager server: %v",
					err,
				),
			)
			if i == (max_tries - 1) {
				// Connection fails on last retry
				req.clientLogger.ClientLogError(
					"method",
					"ClientRequestObject.ConnectToServer",
					"All dialing tries to server failed.",
				)
				// If connection fails on last retry, be sure to close all resources
				//   the ClientRequestObject uses (i.e., server connections, resources
				//   of the ClientLoggingObject).
				req.Close()
				return err
			}
		} else {
			// If dial is successful, break from for loop and set up RPC connection below.
			break
		}
	}

	req.clientLogger.ClientLogInfo(
		"method",
		"ClientRequestObject.ConnectToServer",
		"Dialing is successful.  RPC stream connection to server is being created now...",
	)
	req.rpcConn = rpc.NewConn(rpc.NewStreamTransport(req.regConn), nil)
	req.clientLogger.ClientLogInfo(
		"method",
		"ClientRequestObject.ConnectToServer",
		"RPC stream connection to server is created",
	)
	req.jobMgrCap = jobmgrcapnp.JobExecutor(req.rpcConn.Bootstrap(context.Background()))
	req.clientLogger.ClientLogInfo(
		"method",
		"ClientRequestObject.ConnectToServer",
		"Bootstrap jobmgrcapnp.JobExecutor service capability received from the Job Manager server",
	)

	// Create a new JobCallbackHandler that the client itself will use to log specific job info in all
	//   of the logs (client activity and job-specific logs), using its RPCs.
	req.jobCallbackHandler = NewJobCallbackHandler(req.jobName, req.process, req.clientLogger)
	req.clientLogger.ClientLogInfo(
		"method",
		"ClientRequestObject.ConnectToServer",
		fmt.Sprintf(
			"A new JobCallbackHandler has been created for job '%s', which will run process '%s'.",
			req.jobName,
			req.process,
		),
	)

	// Create a new JobCallback capability that the client will use to send to the server.  Server will
	//   use this capability to call this capability's RPCs and return information to store and log
	//   regarding the specific job.
	req.jobCallbackCap = jobmgrcapnp.JobCallback_ServerToClient(req.jobCallbackHandler)
	req.clientLogger.ClientLogInfo(
		"method",
		"ClientRequestObject.ConnectToServer",
		"A new JobCallback capability has also been created for the Cap'n Proto Job Manager server to interact with the JobCallbackHandler during submission and execution of the job.",
	)

	return nil
}

// Submits the job through the jobMgrCap capability's SubmitJob() RPC, and receives back, stores, and uses a
//   JobQueryCancel capability to query and/or cancel the submitted job.
func (req *ClientRequestObject) SubmitJob() error {
	defer req.Close()

	req.clientLogger.ClientLogInfo(
		"method",
		"ClientRequestObject.SubmitJob",
		fmt.Sprintf(
			"Now submitting job '%s' which will run process '%s' by calling the JobExecutor_submitJob RPC.",
			req.jobName,
			req.process,
		),
	)

	// Submit the job, wait for job to submit and for client to get the jobmgrcapnp.JobQueryCancel object.
	rpcCtx := context.Background()
	// Get promise of submitting the job
	submitJobPromise, submitJobPromRel := req.jobMgrCap.SubmitJob(
		rpcCtx,
		func(p jobmgrcapnp.JobExecutor_submitJob_Params) error {
			jobInfoParErr := p.SetJobinfo(req.jobSubmission)
			if jobInfoParErr != nil {
				return jobInfoParErr
			}
			jobCallbackParErr := p.SetCallback(req.jobCallbackCap)
			if jobCallbackParErr != nil {
				return jobInfoParErr
			}
			return nil
		},
	)
	defer submitJobPromRel()
	// Use promise of submitting the job to now actually submit the job and receive back
	//   a results struct with an actual JobQueryCancel to control the newly submitted job.
	submitJobResults, err := submitJobPromise.Struct()
	if err != nil {
		req.clientLogger.ClientLogError(
			"method",
			"ClientRequestObject.SubmitJob",
			fmt.Sprintf(
				"Client unable to submit job '%s' with process '%s' into Job Manager service: %v",
				req.jobName,
				req.process,
				err,
			),
		)
		return err
	}
	// Actual JobQueryCancel capability is received.
	req.jobQueryCancel = submitJobResults.Jobquery()

	req.clientLogger.ClientLogInfo(
		"method",
		"ClientRequestObject.SubmitJob",
		fmt.Sprintf(
			"Client has now successfully submitted job '%s' with process '%s'.  Client has received back from the Job Manager server a jobmgrcapnp.JobQueryCancel object.  Now monitoring the job...",
			req.jobName,
			req.process,
		),
	)

	// Setup of local client job timeout context, jobCtx
	var jobCtx context.Context
	var jobCancel context.CancelFunc
	if req.timeout <= 0 {
		jobCtx = context.Background()
		jobCancel = nil
	} else {
		jobCtx, jobCancel = context.WithTimeout(context.Background(), time.Duration(req.timeout) * time.Second)
	}
	defer jobCancel()

	// Set up of job query/monitoring ticker
	queryInterval := 500 * time.Millisecond
	queryTicker := time.NewTicker(queryInterval)
	defer queryTicker.Stop()

	// Goroutine for periodically querying the job.  During the query (aka monitoring) procedure,
	//   any logging of the job activity is performed from this request stub's associated jobCallbackHandler,
	//   which handles RPC calls from the Job Manager server (sent via a req.jobCallbackCap client capability
	//   to interact with this handler).
	go func() {
		// Client calls jobQueryCancel.QueryJob() to get promise of whether job is still active or not.
		jobIsActivePromise, jqRel := req.jobQueryCancel.QueryJob(
			context.Background(),
			nil,
		)
		defer jqRel()
		for {
			select {
			case <-jobCtx.Done():
				// Indicates that the client's maximum wait time for the job to complete has reached or exceeded,
				//   and job needs to be cancelled (that is, client interference is needed).
				// Automatically ignored if jobCtx == context.Background()
				//***********************************************************************************************
				// Use req.clientLogger to log that the jobCtx has timed out, and the job will be cancelled using
				//   req.jobQueryCancel.CancelJob() RPC.
				req.clientLogger.ClientLogInfo(
					"method",
					"ClientRequestObject.SubmitJob",
					fmt.Sprintf(
						"Client has timed out for completion of '%s' with process '%s'.  Client is now about to call the CancelJob() RPC from its req.jobQueryCancel capability to cancel job.",
						req.jobName,
						req.process,
					),
				)
				// Use req.jobQueryCancel.CancelJob() RPC to cancel the submitted job.  This RPC should also send to
				//   the req.jobCallbackHandler a final log message that will be sent to the req.clientLogger to add
				//   to its client activity log and job activity log by calling the sent capability's jobCancelled() 
				//   RPC.
				jobIsCancelledPromise, jobIsCancRel := req.jobQueryCancel.CancelJob(
					context.Background(),
					nil,
				)
				defer jobIsCancRel()
				jobIsCancelled, err := jobIsCancelledPromise.Struct()
				if err != nil {
					req.clientLogger.ClientLogError(
						"method",
						"ClientRequestObject.SubmitJob",
						fmt.Sprintf(
							"Error in receiving job cancellation signal for job '%s' running process '%s' using JobQueryCancel.CancelJob RPC: %v",
							req.jobName,
							req.process,
							err,
						),
					)
				}
				req.clientLogger.ClientLogInfo(
					"method",
					"ClientRequestObject.SubmitJob",
					fmt.Sprintf(
						"Checking if CancelJob() RPC successfully cancelled the job: %v",
						jobIsCancelled.Cancelled(),
					),
				)
				// Goroutine returns/exits.
				return
			case <-req.jobCallbackHandler.doneChan:
				// Indicates that the job has succeeded or failed on its own, without any client 
				//   interference.
				//********************************************************************************
				// Use req.clientLogger to log query process exit.
				req.clientLogger.ClientLogInfo(
					"method",
					"ClientRequestObject.SubmitJob",
					fmt.Sprintf(
						"Job '%s' with process '%s' has completed without any client interference for cancellation of the job.",
						req.jobName,
						req.process,
					),
				)
				// Goroutine returns/exits.
				return
			case <-queryTicker.C:
				// Empty, means query just outside the select statement is called.
			}
			// Query to the job is made using the promise recieved at beginning of query process.
			jobIsActive, err := jobIsActivePromise.Struct()
			if err != nil {
				req.clientLogger.ClientLogError(
					"method",
					"ClientRequestObject.SubmitJob",
					fmt.Sprintf(
						"Error in calling on JobQueryCancel.QueryJob RPC to get active/inactive job status for job '%s' running process '%s': %v",
						req.jobName,
						req.process,
						err,
					),
				)
			}
			// Get jobIsActive.Active() to print out whether job is actually active or not
			req.clientLogger.ClientLogInfo(
				"method",
				"ClientRequestObject.SubmitJob",
				fmt.Sprintf(
					"Checking that job '%s' with process '%s' is still active: %v",
					req.jobName,
					req.process,
					jobIsActive.Active(),
				),
			)
		}
	}()

	// Main method body, in the meantime, waits for job to be completed (its doneChan to receive a signal), and when
	//   it gets that signal, checks the status of the job, and logs into the client and job logs an
	//   appropriate message based on status.
	<-req.jobCallbackHandler.doneChan
	// Allow query goroutine to fully process final info and exit completely.
	time.Sleep(2 * time.Second)
	// Log into client logs a final message that the job was able to be either successfully cancelled
	//   by the client, or able to complete on its own.
	var completionMsg string
	switch req.jobCallbackHandler.status {
	case jobmgrcapnp.Status_error:
		completionMsg = fmt.Sprintf(
			"Job '%s' with process '%s' completed with errors.",
			req.jobName,
			req.process,
		)
	case jobmgrcapnp.Status_cancel:
		completionMsg = fmt.Sprintf(
			"Job '%s' with process '%s' was successfully cancelled by the client after %d-second timeout.",
			req.jobName,
			req.process,
			req.timeout,
		)
	case jobmgrcapnp.Status_success:
		completionMsg = fmt.Sprintf(
			"Job '%s' with process '%s' completed successfully.",
			req.jobName,
			req.process,
		)
	}
	
	req.clientLogger.ClientLogInfo(
		"method",
		"ClientRequestObject_SubmitJob",
		completionMsg,
	)

	// A nil error cancellation indicates the entire job submission, query, and/or cancellation
	//   procedure has been successful.  There have been no processing errors in any of the
	//   client's requests within the execution of this method.
	return nil
}

// Close any open resources, such as when SubmitJob() method is completed.
//   Deferred in client code.
func (req *ClientRequestObject) Close() error {
	// Client RPC connection obtained in order to close connection.
	// rpcConn field is changed to nil, and RPC connection is closed.
	conn := req.rpcConn
	req.rpcConn = nil
	conn.Close()
	// Close the general connection that was wrapped in the RPC connection.
	req.regConn.Close()
	// Close all remaining open resources in the req.clientLogger instance.
	req.clientLogger.Close()
	return nil
}