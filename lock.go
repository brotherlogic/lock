package main

import (
	"fmt"
	"sync"

	"github.com/brotherlogic/goserver"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pbg "github.com/brotherlogic/goserver/proto"
	pb "github.com/brotherlogic/lock/proto"
)

//Server main server type
type Server struct {
	*goserver.GoServer
	mlock *sync.Mutex
}

// Init builds the server
func Init() *Server {
	s := &Server{
		GoServer: &goserver.GoServer{},
		mlock:    &sync.Mutex{},
	}
	s.NeedsLead = true
	return s
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {
	pb.RegisterLockServiceServer(server, s)
}

// ReportHealth alerts if we're not healthy
func (s *Server) ReportHealth() bool {
	return true
}

// Shutdown the server
func (s *Server) Shutdown(ctx context.Context) error {
	return nil
}

// Mote promotes/demotes this server
func (s *Server) Mote(ctx context.Context, master bool) error {
	return nil
}

// GetState gets the state of the server
func (s *Server) GetState() []*pbg.State {
	return []*pbg.State{}
}

func main() {
	server := Init()
	server.PrepServer("lock")
	server.Register = server

	err := server.RegisterServerV2(false)
	if err != nil {
		return
	}

	fmt.Printf("%v", server.Serve())
}
