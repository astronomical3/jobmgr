package client

import (
	"flag"
	"fmt"
)

//********************************************************************************
// Definition of the object supporting an execution of the command for submitting
//   a job into the Cap'n Proto Job Manager Service using the command-line
//   interface.
type Cli struct {
	// For connection to server using "addr" and "port" flags.
	address      *string
	port         *int

	// For logging CLI object activity in the client activity logs.
	clientLogger *ClientLoggingObject

	// For passing actual job information from the flags to the request stub,
	//   representing the individual job to submit.
	jobName      *string
	process      *string
	timeout      *int
}

// Constructor function that creates a new instance of a Cli object.  The
//   first object added to the CLI command object is an instance of a 
//   *ClientLoggingObject so that it can log its own activity, and also
//   pass the pointer to this object down to the core ClientRequestObject
//   (and objects that core struct has access to, such as job callback 
//   handler).
func NewCli(clientLogger *ClientLoggingObject) *Cli {
	return &Cli{clientLogger: clientLogger}
}

// Method of the Cli object that loads the values of the flags of the `go run [jobmgr/client/]main.go` command.
func (cli *Cli) LoadFlagsAndParseCmd() {
	// Loading address and port flag values for connecting to the Job Manager Service server.
	cli.address = flag.String("addr", "localhost", "address of the Job Manager Service server")
	cli.port = flag.Int("port", 50051, "port of the Job Manager Service server")

	// Loading job submission information for the actual job submission procedure.
	cli.jobName = flag.String("jobname", "sampleJob", "user-defined name of the job to submit to, and run on, the Job Manager Service server")
	cli.process = flag.String("process", "countTo10", "actual process that the submitted job will run on the Job Manager Service server")
	cli.timeout = flag.Int("timeout", 0, "maximum number of seconds the client will wait before it decides to cancel the job.  0 means run job to completion.")

	flag.Parse()
}

// Method of the Cli object that actually performs the submit job procedure by creating a 
//   jobmgrcapnp.JobSubmitInfo struct, then a ClientRequestObject, then uses the 
//   ClientRequestObject's methods to first ConnectToServer() then SubmitJob().  Error will
//   be returned from either of those 2 methods.  (ClientRequestObject methods 
//   should be able to take care of the closings of its resources by itself, using its own
//   Close() method.)
func (cli *Cli) SubmitJob() error {
	// CLI object creates a jobmgrcapnp.JobSubmitInfo struct.
	cli.clientLogger.ClientLogInfo(
		"method",
		"Cli.SubmitJob",
		fmt.Sprintf(
			"Creating a jobmgrcapnp.JobSubmitInfo struct for job '%s', which will run process '%s'",
			*cli.jobName,
			*cli.process,
		),
	)
	
	// CLI object creates a ClientRequestObject to pass along the job submission info
	//   and create a new request to submit a new job into the Job Manager Service 
	//   server.  This object is the CORE client object, as this actually connects to 
	//   the server and submit and monitor the job.
	clientRequest := NewRequest(
		*cli.jobName,
		*cli.process,
		*cli.address,
		string(rune(*cli.port)),
		*cli.timeout,
		cli.clientLogger,
	)

	// New core client object now connects to the JobManagerService server.  The
	//   ClientLoggerObject will log any error at the ConnectToServer() level, and
	//   the CLI object here just receives back either an error (if connection error
	//   occurs) or a nil error (if connection is successful).
	err := clientRequest.ConnectToServer()
	if err != nil {
		return err
	}

	// New core client object now actually submits the job into the Job Manager 
	//   Service server, and monitors the job as the server continues to return
	//   back occasionally any log messages regarding progress of the job.  The
	//   client object will log any error at the SubmitJob() level, and the CLI
	//   object here just receives back the error.
	err = clientRequest.SubmitJob()
	if err != nil {
		return err
	}

	// A nil error indicates the entire job submission process by the core client
	//   object went successfully.
	return nil
}