package main

import (
	"fmt"

	"golang.org/x/net/context"

	pb "github.com/brotherlogic/lock/proto"
)

func (s *Server) AcquireLock(ctx context.Context, req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (s *Server) ReleaseLock(ctx context.Context, req *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, error) {
	return nil, fmt.Errorf("Not implemetned")
}
