package main

import (
	"log"

	"github.com/astronomical3/jobmgr/client"
)

// Path relative to the root client activity logging directory path (which is 'jobmgr/client/clientlogs/')
const clientLogFilepath = "client.log"

func main() {
	// Create a ClientLoggingObject to start logging different activity throughout the client application.
	clientLogger := client.NewClientLoggingObject(clientLogFilepath)

	// Create a new CLI object that will then take in the flags of the `go run [jobmgr/]client.go` command.
	cliObj := client.NewCli(clientLogger)
	cliObj.LoadFlagsAndParseCmd()
	err := cliObj.SubmitJob()
	if err != nil {
		log.Printf("CLI object's SubmitJob operation ended in error: %v", err)
	}
}