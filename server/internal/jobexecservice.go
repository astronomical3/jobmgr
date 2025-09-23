package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/astronomical3/jobmgr/jobmgrcapnp"
)

//******************************************************************************************************
// Struct that implements the root jobmgrcapnp.JobExecutor interface, and will be converted into the
//   bootstrap capability for the interface when the Cap'n Proto Job Manager Service server is set up
//   and serving clients.
// This handler will contain a map of submitted jobs (each represented by a *JobHandler object), mapping
//   each *JobHandler to a job ID (in string type).
type JobExecutorHandler struct {
	// For assigning each new job to a new job ID
	newJobId int
	// For logging Job Executor Service activity on the server logs
	serverLogger *ServerLoggingObject
	// Map of submitted jobs
	submittedJobs map[string]*JobHandler
	// Ensures safe concurrent access to the submitted jobs in the submittedJobs map.
	mu sync.Mutex
}

// Constructor function that will be used for creating a new JobExecutorHandler.
func NewJobExecutorHandler(serverLogger *ServerLoggingObject) *JobExecutorHandler {
	return &JobExecutorHandler{
		newJobId: 1, // Every time a new Job Executor is set up, set first job ID to 1
		serverLogger: serverLogger,
		submittedJobs: make(map[string]*JobHandler),
	}
}

// Implementation of the jobmgrcapnp.JobExecutor_submitJob() RPC.
func (jeh *JobExecutorHandler) SubmitJob(ctx context.Context, call jobmgrcapnp.JobExecutor_submitJob) error {
	// Get the structure of all arguments of the RPC from the client.
	args := call.Args()

	// Get the jobmgrcapnp.JobSubmitInfo struct, "jobSubmit", and also get
	//   its individual "jobName" and "process" fields.
	jobSubmit, err := args.Jobinfo()
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Error in getting jobmgrcapnp.JobSubmitInfo struct: %v",
				err,
			),
		)
		return err
	}
	jobName, err := jobSubmit.JobName()
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Error in getting jobName field from jobmgrcapnp.JobSubmitInfo struct: %v",
				err,
			),
		)
		return err
	}
	process, err := jobSubmit.Process()
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Error in getting process field from jobmgrcapnp.JobSubmitInfo struct: %v",
				err,
			),
		)
		return err
	}

	// Get the client capability to the client's associated job callback object
	//   that is implementing the jobmgrcapnp.JobCallback interface, so that the
	//   server can continue to inform client of any job updates throughout job
	//   submission procedure here in this RPC, as well as during the actual 
	//   execution of the job's process.
	jobCallbackCap := args.Callback()

	// A new jobID number is received, and converted into a string.  Since multiple clients
	//   may be requesting to submit a job to the server concurrently, the mutex mu that is
	//   usually used for accessing the submittedJobs map is also used for this operation,
	//   too.
	jeh.mu.Lock()
	jobId := string(rune(jeh.newJobId))
	jeh.newJobId++
	jeh.mu.Unlock()

	// Create a new *JobHandler object for the requested job.  This is the beginning of the
	//   process of submitting the job.
	newJob := NewJobHandler(
		jobName,
		jobId,
		process,
		jobCallbackCap,
		jeh.serverLogger,
	)

	// Add the newJob to the submittedJobs map to finalize the process of submitting the new
	//   job.
	jeh.mu.Lock()
	jeh.submittedJobs[jobId] = newJob
	jeh.mu.Unlock()

	// Log to server logs that the job has now been created and submitted into the JobExecutor
	//   service.
	jeh.serverLogger.ServerLogInfo(
		"rpc",
		"jobmgrcapnp.JobExecutor_submitJob",
		fmt.Sprintf(
			"Job '%s' with process '%s' has been submitted into the Job Executor Service with job ID '%s'.",
			jobName,
			process,
			jobId,
		),
	)

	// Obtain the job with the "jobID" again through the submittedJobs map.
	jeh.mu.Lock()
	newlySubmittedJob, exists := jeh.submittedJobs[jobId]
	if !exists {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Failed to retrieve submitted job with job ID %s from the map of submitted jobs",
				jobId,
			),
		)
		return errors.New("could not retrieve newly submitted job")
	}

	// Use the job's jobmgrcapnp.JobCallback_initialUpdateJobInfo() RPC to
	//   send back to the client's job callback the job name, job ID, and an 
	//   process name.
	initialJobUpdatePromise, initialJobUpdateRel := newlySubmittedJob.jobCallback.InitialUpdateJobInfo(
		context.Background(),
		func(p jobmgrcapnp.JobCallback_initialUpdateJobInfo_Params) error {
			err := p.SetJobId(newlySubmittedJob.jobId)
			if err != nil {
				return err
			}

			err = p.SetProcess(newlySubmittedJob.process)
			if err != nil {
				return err
			}

			err = p.SetJobname(newlySubmittedJob.jobName)
			if err != nil {
				return err
			}

			return nil
		},
	)
	defer initialJobUpdateRel()

	// Also, update the status of the job to "submit" using updateStatus() RPC
	initialJobStatusUpdatePromise, initialJobStatRel := newlySubmittedJob.jobCallback.UpdateStatus(
		context.Background(),
		func(p jobmgrcapnp.JobCallback_updateStatus_Params) error {
			p.SetStatus(newlySubmittedJob.status)
			return nil
		},
	)
	defer initialJobStatRel()

	// Actually call the initialUpdateJobInfo() and updateStatus() RPCs to provide
	//   the job callback on the client side the initial job submission info, 
	//   including the "submit" status.
	_, err = initialJobUpdatePromise.Struct()
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Failed to send initial job info update information to client's job callback for job '%s' with job ID %s: %v",
				newlySubmittedJob.jobName,
				newlySubmittedJob.jobId,
				err,
			),
		)
		return err
	}

	_, err = initialJobStatusUpdatePromise.Struct()
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Failed to send initial job status update information to client's job callback for job '%s' with job ID %s: %v",
				newlySubmittedJob.jobName,
				newlySubmittedJob.jobId,
				err,
			),
		)
		return err
	}

	// Use the NewProcessChoices() map to select the appropriate process that the job will run, 
	//   based on the "process" attribute used as key.  This will become the variable/function
	//   named "processToRun".
	possibleProcessesToRun := NewProcessChoices(newlySubmittedJob)
	processToRun, exists := possibleProcessesToRun[newlySubmittedJob.process]
	if !exists {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			"Process requested by client does not exist in process choice map",
		)
		return errors.New("process requested by client not in process choice map")
	}

	// Send back capability to client for the jobmgrcapnp.JobQueryCancel interface,
	//   using the newlySubmittedJob, so that the submitted job will be controlled
	//   by the JobQueryCancel that the client gets.
	results, err := call.AllocResults()
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Failed to execute call.AllocResults: %v",
				err,
			),
		)
		return err
	}
	jobQcCap := jobmgrcapnp.JobQueryCancel_ServerToClient(newlySubmittedJob)
	err = results.SetJobquery(jobQcCap)
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Failed to send over the client capability for jobmgrcapnp.JobQueryCancel for job ID %s: %v",
				jobId,
				err,
			),
		)
		return err
	}

	// Record the start time of the new job/process to run
	newlySubmittedJob.startTime = time.Now()

	// Start up the processToRun() function in a goroutine.  This will be the actual process
	//   controlled and monitored by the client's newly received capability to the 
	//   jobmgrcapnp.JobQueryCancel interface (which itself is implemented by an instance
	//   of the *JobHandler).
	go processToRun()

	// Record that the job has started running in the server logs
	jeh.serverLogger.ServerLogInfo(
		"rpc",
		"jobmgrcapnp.JobExecutor_submitJob",
		fmt.Sprintf(
			"Process '%s' has started running for job '%s' (job ID %s).",
			newlySubmittedJob.process,
			newlySubmittedJob.jobName,
			newlySubmittedJob.jobId,
		),
	)

	// Record that the job has started running in the client logs, 
	//   and change job's status to "exec".
	updateJobLogExecPromise, updateLogExecRel := newlySubmittedJob.jobCallback.UpdateJobLog(
		context.Background(),
		func(p jobmgrcapnp.JobCallback_updateJobLog_Params) error {
			err := p.SetLogMsg(
				fmt.Sprintf(
					"Job '%s' with process '%s' (job ID %s) started on %v-%v-%v at %v:%v:%v",
					newlySubmittedJob.jobName,
					newlySubmittedJob.process,
					newlySubmittedJob.jobId,
					newlySubmittedJob.startTime.Year(),
					newlySubmittedJob.startTime.Month(),
					newlySubmittedJob.startTime.Day(),
					newlySubmittedJob.startTime.Hour(),
					newlySubmittedJob.startTime.Minute(),
					newlySubmittedJob.startTime.Second(),
				),
			)
			if err != nil {
				return err
			}

			return nil
		},
	)
	defer updateLogExecRel()

	_, err = updateJobLogExecPromise.Struct()
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Failed to update job callback capability for job ID %s regarding log message beginning of process execution: %v", 
				newlySubmittedJob.jobId,
				err,
			),
		)
		return err
	}

	newlySubmittedJob.status = jobmgrcapnp.Status_exec
	updateStatusExecPromise, updateStatExecRel := newlySubmittedJob.jobCallback.UpdateStatus(
		context.Background(),
		func(p jobmgrcapnp.JobCallback_updateStatus_Params) error {
			p.SetStatus(newlySubmittedJob.status)
			return nil
		},
	)
	defer updateStatExecRel()

	_, err = updateStatusExecPromise.Struct()
	if err != nil {
		jeh.serverLogger.ServerLogError(
			"rpc",
			"jobmgrcapnp.JobExecutor_submitJob",
			fmt.Sprintf(
				"Failed to update job callback capability for job ID %s regarding 'exec' status of the job: %v", 
				newlySubmittedJob.jobId,
				err,
			),
		)
		return err		
	}

	// Error of nil indicates submission process was entirely successful.
	return nil
}