package main

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	dspb "github.com/brotherlogic/dstore/proto"
	pb "github.com/brotherlogic/lock/proto"
)

const (
	KEY = "github.com/brotherlogic/lock/locks"
)

func (s *Server) generateLockKey() string {
	return fmt.Sprintf("%v-%v", s.Registry.Identifier, time.Now().UnixNano())
}

func (s *Server) AcquireLock(ctx context.Context, req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	conn, err := s.FDialServer(ctx, "dstore")
	defer conn.Close()
	if err != nil {
		return nil, err
	}
	client := dspb.NewDStoreServiceClient(conn)
	rresp, err := client.Read(ctx, &dspb.ReadRequest{Key: KEY})
	if err != nil {
		if status.Convert(err).Code() != codes.NotFound {
			return nil, err
		}
	}

	locks := &pb.Locks{}

	//Only unmarshal if we actually read something
	if status.Convert(err).Code() != codes.NotFound {
		proto.Unmarshal(rresp.GetValue().GetValue(), locks)
	}

	lock := &pb.Lock{
		AcquireTime: time.Now().Unix(),
		ReleaseTime: time.Now().Unix() + req.GetLockDuration(),
		Key:         req.GetKey(),
		LockKey:     s.generateLockKey(),
	}

	// Check that we don't already have this lock
	var nlocks []*pb.Lock
	for _, exLock := range locks.GetLocks() {
		if exLock.GetKey() == req.GetKey() {
			if exLock.GetReleaseTime() > lock.GetAcquireTime() {
				return nil, status.Errorf(codes.AlreadyExists, "This key is locked until %v", time.Unix(exLock.GetReleaseTime(), 0))
			}
		} else {
			nlocks = append(nlocks, exLock)
		}
	}
	nlocks = append(nlocks, lock)
	locks.Locks = nlocks

	data, err := proto.Marshal(locks)
	if err != nil {
		return nil, err
	}

	wresp, err := client.Write(ctx, &dspb.WriteRequest{
		Key:   KEY,
		Value: &anypb.Any{Value: data},
	})

	if err != nil {
		return nil, err
	}

	if wresp.GetConsensus() < 1.0 {
		return nil, fmt.Errorf("could not get consensus on write (%v)", wresp.GetConsensus())
	}

	return &pb.AcquireLockResponse{Lock: lock}, nil
}

func (s *Server) ReleaseLock(ctx context.Context, req *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, error) {
	return nil, fmt.Errorf("not implemetned")
}
