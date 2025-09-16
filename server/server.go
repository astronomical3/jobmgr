package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/astronomical3/jobmgr/server/internal"
)

// Provide a fill path to the server.log file, which will be used for logging all
//   server application activity history using the ServerLoggingObject.
const serverLogFilepath = "jobmgr/server/serverlogs/server.log"

func main() {
	// Get the flags to the `go run [jobmgr/server/]server.go` command for the address and
	//   port of the listener to the server, and create the TCP address string
	//   using those flags.
	lisAddr := flag.String(
		"addr",
		"localhost",
		"address for the Job Manager Service server to listen to requests to",
	)
	lisPort := flag.Int(
		"port",
		50051,
		"port for the Job Manager Service server to listen to requests to",
	)
	flag.Parse()
	tcpAddr := fmt.Sprintf("%s:%d", *lisAddr, *lisPort)

	// Create a new ServerLoggingObject
	serverLogger := internal.NewServerLoggingObject(serverLogFilepath)

	// Create a new net.Listener object.  If the creation of the listener 
	//   results in an error, log the error in the serverLogger, and also close
	//   the serverLogger and exit the program.
	lis, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		serverLogger.ServerLogError(
			"file",
			"server.go",
			fmt.Sprintf("net.Listener failed to listen: %v", err),
		)
		serverLogger.Close()
		lis.Close()
		return
	}
	
	// Create a new JobExecutorHandler, which the Job Manager Service server will
	//   send over a bootstrap capability to, when the server starts listening to
	//   requests at the provided listener's address and port.
	jobExecutorHandler := internal.NewJobExecutorHandler(serverLogger)

	// Create a new Job Manager Service server, which will host the Job Executor
	//   client capability as the bootstrap capability for submitting jobs.
	jobManagerServer := internal.NewJobManagerServer(lis, jobExecutorHandler, serverLogger)

	// Create a serverDone channel that will close whenever a server error is resulted
	//   (e.g., failed to start up server, listener closed due to signal channel 
	//   triggering Shutdown()).
	// Also, create a sigChan that will listen to any shutdown signal (via an OS
	//   interruption or termination signal) while the actual server is listening to
	//   requests in a separate goroutine.
	serverDone := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create a separate goroutine to try and start up the Job Manager Service server,
	//   so that the server can now listen to requests coming in from clients.  If an
	//   error results at any point while the server is being set up, or the server is
	//   trying to listen to client requests, the serverDone channel is closed, sending 
	//   a close signal to the main function body to indicate full graceful shutdown.
	go func() {
		log.Printf("Server has failed to serve: %v", jobManagerServer.ListenAndServe())
		close(serverDone)
	}()

	// Main function body, in the meantime, will listen for the OS interruption or termination
	//   signal to shut down the Job Manager Service server and its resources.
	<-sigChan
	jobManagerServer.Shutdown()

	// Listen for the signal that the ListenAndServe() method has fully exited and returned
	//   and error, which should then trigger a signal through the serverDone channel and
	//   that the server has gracefully shutdown.
	<-serverDone
	log.Printf("Server has gracefully shutdown.")
}