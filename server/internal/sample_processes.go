// Example processes that can be defined in the JobHandler struct to run when the individual job is submitted.

//*************************************************************************************************************
// BEST TO NOT DELETE OR CHANGE THIS FILE, AS THIS FILE CAN SERVE AS A GUIDELINE ON HOW TO STRUCTURE A RUNNING
//   PROCESS.
// Instead, use a different file (or set of files) in the server package to define your own processes to run.
// Also, simply comment out the references to the sample processes in the Process Choices map in the
//   process_choices.go file, so that they serve as a guideline on how to register your own processes.
//*************************************************************************************************************

package internal

import (
	"fmt"
	"os"
	"time"

	"github.com/astronomical3/jobmgr/jobmgrcapnp"
)

// const countTo10FilePrefix = "jobmgr/server/jobresults/countTo10-"

// Basic iterative process that counts to 10.
func (j *JobHandler) countTo10() {
	// Create new filename
	startDateTime := fmt.Sprintf(
		"%v_%v_%v-%v%v%v",
		j.startTime.Year(),
		j.startTime.Month(),
		j.startTime.Day(),
		j.startTime.Hour(),
		j.startTime.Minute(),
		j.startTime.Second(),
	)
	// countTo10Filename := countTo10FilePrefix + startDateTime + ".txt"
	countTo10Filename := startDateTime + ".txt"

	// Create the file to perform the countTo10 process on.
	countTo10FileObj, jobErr := os.OpenFile(
		countTo10Filename,
		os.O_WRONLY|os.O_CREATE|os.O_APPEND,
		0600,
	)

	// If the file fails to open, that is a job error to try and record both on the server
	//   side, and also to send that message to the client side.  Also, this should change
	//   the job's status to "error".
	if jobErr != nil {
		j.JobErrorHelper(
			jobErr,
			fmt.Sprintf(
				"Job '%s' running process 'countTo10' (job ID %s) failed due to open file error in creating file %s: %v",
				j.jobName,
				j.jobId,
				countTo10Filename,
				jobErr,
			),
		)
	}

	beginningString := fmt.Sprintf("~~~~~~~~~Start Time: %s~~~~~~~~\n", startDateTime)
	_, jobErr = countTo10FileObj.Write([]byte(beginningString))
	if jobErr != nil {
		j.JobErrorHelper(
			jobErr,
			fmt.Sprintf(
				"Job '%s' running process 'countTo10' (job ID %s) failed due to open file error in creating file %s: %v",
				j.jobName,
				j.jobId,
				countTo10Filename,
				jobErr,
			),
		)
	}

	for i := 0; i < 10; i++ {
		// Per iteration, check first that the client has not cancelled the process/job
		//   or abruptly ended on its own (that is, the j.doneChan has not received a
		//   close signal).
		select {
		case <-j.doneChan:
			// If the reason for the doneChan being closed unexpectedly is because
			//   the client issued a cancellation, log on the server that the
			//   process is cancelled.
			if j.status == jobmgrcapnp.Status_cancel {
				j.endTime = time.Now()
				j.duration = j.endTime.Sub(j.startTime)
				j.serverLogger.ServerLogWarn(
					"job",
					j.jobName,
					fmt.Sprintf(
						"Job '%s' running process 'countTo10' (job ID %s) has been cancelled per client request.",
						j.jobName,
						j.jobId,
					),
				)
			}
		default:
			// If client has not issued cancellation, continue on with next iteration.
			//   Try and write the new iteration's number into the new file.  Any error
			//   will be logged in on the server, as well as the client side, as a job
			//   error, and will set the job status to "error".
			newNumString := fmt.Sprintf("%d\n", i + 1)
			if _, jobErr := countTo10FileObj.Write([]byte(newNumString)); jobErr != nil {
				// Ignore the close error, as write error takes precedence as the main
				//   job error here.
				countTo10FileObj.Close()
				j.JobErrorHelper(
					jobErr,
					fmt.Sprintf(
						"Job '%s' running process 'countTo10' (job ID %s) failed due to write error in file %s: %v", 
						j.jobName,
						j.jobId,
						countTo10Filename,
						jobErr,
					),
				)
				// End the job
				return
			}

			// If write was successful, a call of j.JobUpdateHelper() is used here to log on
			//   the server and client sides the update of another successful iteration, and 
			//   the loop waits 500ms for the next iteration.
			j.JobUpdateHelper(
				fmt.Sprintf(
					"Process 'countTo10' (job ID %s) has completed iteration %d with result %d",
					j.jobId,
					i,
					i + 1,
				),
			)
			time.Sleep(500 * time.Millisecond)
		}
	}

	processEnd := time.Now()
	processEndString := fmt.Sprintf(
		"%v_%v_%v-%v%v%v",
		processEnd.Year(),
		processEnd.Month(),
		processEnd.Day(),
		processEnd.Hour(),
		processEnd.Minute(),
		processEnd.Second(),	
	)
	endString := fmt.Sprintf("~~~~~~~~~End Time: %s~~~~~~~~\n", processEndString)
	_, jobErr = countTo10FileObj.Write([]byte(endString))
	if jobErr != nil {
		j.JobErrorHelper(
			jobErr,
			fmt.Sprintf(
				"Job '%s' running process 'countTo10' (job ID %s) failed due to open file error in creating file %s: %v",
				j.jobName,
				j.jobId,
				countTo10Filename,
				jobErr,
			),
		)
	}

	// Now that the loop has fully and successfully executed, try to close the new 
	//   countTo10 file.  If it cannot close, the error will be logged on both the
	//   server and client side as a job error, and will set the job status to 
	//   "error".
	if jobErr := countTo10FileObj.Close(); jobErr != nil {
		j.JobErrorHelper(
			jobErr,
			fmt.Sprintf(
				"Job '%s' running process 'countTo10' (job ID %s) failed due to close error in file %s: %v",
				j.jobName,
				j.jobId,
				countTo10Filename,
				jobErr,
			),
		)
		// End the job here
		return
	}

	// If the job completes, and the file closes successfully, call the job's callback capability's 
	//   jobSuccessful() RPC, and remember to dismiss the capability, all by using the
	//   job's JobSuccessHelper() method.
	j.JobSuccessHelper(
		fmt.Sprintf(
			"Job '%s' running process 'countTo10' (job ID %s) was successful",
			j.jobName,
			j.jobId,
		),
	)
}