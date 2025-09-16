package internal

import (
	"fmt"
	"net"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/astronomical3/jobmgr/jobmgrcapnp"
)

//*********************************************************************************
// Struct that holds the General Cap'n Proto Server for the Job Manager Service.
type JobManagerServer struct {
	listener       net.Listener
	jobExecutorCap jobmgrcapnp.JobExecutor
	serverLogger   *ServerLoggingObject
}

// Constructor function that creates a new instance of the JobManagerServer.
func NewJobManagerServer(lis net.Listener, jobExecutorHandler *JobExecutorHandler, serverLogger *ServerLoggingObject) *JobManagerServer {
	return &JobManagerServer{
		listener:       lis, 
		jobExecutorCap: jobmgrcapnp.JobExecutor_ServerToClient(jobExecutorHandler),
		serverLogger:   serverLogger,
	}
}

// Method of the Cap'n Proto Job Manager Service server that sets up the server,
//   listens for ongoing requests, sets up RPC connections, and serves the
//   jobmgrcapnp.JobManager client capability to accepted clients.
func (s *JobManagerServer) ListenAndServe() error {
	defer s.serverLogger.Close()

	s.serverLogger.ServerLogInfo(
		"method",
		"JobManagerServer.ListenAndServe",
		fmt.Sprintf("Listening on address %v", s.listener.Addr()),
	)

	// For each client connection to the server, attempt to accept the 
	//   client connection, and for a successful connection to the client,
	//   start up a goroutine that creates an RPC strea, transport 
	//   connection, so then the server can serve the client capability of
	//   the root jobmgrcapnp.JobExecutor interface to the client.  The
	//   goroutine is kept alive so that the server can continue to communicate
	//   with the client until the connection is finished (e.g., client code
	//   exits, client is cut off suddenly, etc.)
	// If any client connection cannot be accepted, an error is returned, and
	//   the server is automatically stopped.
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.serverLogger.ServerLogError(
				"method",
				"JobManagerServer.ListenAndServe",
				fmt.Sprintf("Error accepting client connection: %v", err),
			)
			return err
		}

		go func() {
			rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), &rpc.Options{
				BootstrapClient: capnp.Client(s.jobExecutorCap).AddRef(),
			})
			<-rpcConn.Done()
		}()
	}
}



// Method of the Cap'n Proto Job Manager Service server that releases the
//   jobmgrcapnp.JobExecutor client capability in order to shutdown the 
//   server.  Also closes all remaining open resources that the 
//   attached ServerLoggingObject is using (e.g., server activity history
//   log file) using its Close() method, and the server's listener object
//   using that object's own Close() method.
// This method would be deferred right after creating the server using
//   the NewJobManagerServer() constructor, so that regardless what happens
//   to the server that causes it to shut down, it will always perform the
//   cleanup procedures here.
// This method issues the listener's Close() method, so that the listener
//   close error can still be logged in the ListenAndServe() method, and 
//   then the returning of the error from the ListenAndServe() method also
//   triggers the Close() of the serverLogger from within that method.
func (s *JobManagerServer) Shutdown() {
	s.serverLogger.ServerLogInfo(
		"method",
		"JobManagerServer.Close",
		"Bootstrap client capability to root jobmgrcapnp.JobExecutor service interface being released, closing listener, and closing server activity logs...",
	)
	s.jobExecutorCap.Release()
	s.listener.Close()
}