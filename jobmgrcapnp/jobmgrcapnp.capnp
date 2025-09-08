using Go = import "/go.capnp";
@0x95f578ba422c9f4b;
$Go.package("jobmgrcapnp");
$Go.import("jobmgrcapnp");

#---------------------------------------------------------------------
# Job status enumerator
enum Status {
    submit  @0; # Job successfully submitted
    exec    @1; # Job in progress
    error   @2; # Job ended in error
    success @3; # Job ended successfully
    cancel  @4; # Job was cancelled by client
}

#----------------------------------------------------------------------
# Client callback capability for receiving updates on a submitted job
interface JobCallback {
    # Send back first pieces of submitted job info for client callback
    #   object to receive and log.
    initialUpdateJobInfo @0 (jobname :Text, jobId :Text, process :Text);
    # Send back to the client's submitted job's log updates on the 
    #   progress of the job's process, for the client to log on that
    #   job log.
    updateJobLog         @1 (logMsg :Text);
    # Send back to the client's submitted job's callback the status
    #   that the job/process is now running.
    updateStatus         @2 (status :Status);
    # Send back to the client's submitted job's callback that the job
    #   completed in "success", and to its job log a log message that
    #   that the job was successful.
    jobSuccessful        @3 (logMsg :Text, status :Status);
    # Send back to the client's submitted job's callback that the job
    #   completed in "error", and to its job log a log message that the
    #   job failed, with the error included.
    jobFailed            @4 (logMsg :Text, status :Status);
    # Send back to the client's submitted job's callback that the job
    #   went to "cancel" status because the client decided to cancel the
    #   job, and to its job log a log message explaining that the job
    #   was cancelled.
    jobCancelled         @5 (logMsg :Text, status :Status);
}


#------------------------------------------------------------------------
# Information the client will add in about the job it requests to run.
struct JobSubmitInfo {
    jobName @0 :Text;  # User-defined specific name of the job to run.
    process @1 :Text;  # Actual process the job will execute once submitted.
}


#------------------------------------------------------------------------
# Job Executor capability for actually submitting a job
interface JobExecutor {
    # Submit the job with the desired name and process to run, as well as
    #   a JobCallback to return loggable and updatable status information
    #   about the job/process throughout the duration of the execution
    #   once the job is submitted.
    submitJob @0 (jobinfo :JobSubmitInfo, callback :JobCallback) -> (jobquery :JobQueryCancel);
}

#------------------------------------------------------------------------
# Job Query/Cancel capability returned to client by the JobExecutor service
#   for querying/monitoring and cancelling the individual job.
interface JobQueryCancel {
    # Monitor/query the job to see if the job is still active.  Client
    #   will call this every certain interval of time.
    queryJob  @0 () -> (active :Bool);
    # Cancel the job.
    cancelJob @1 () -> (cancelled :Bool);
}