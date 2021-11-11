package main

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	dspb "github.com/brotherlogic/dstore/proto"
	gpb "github.com/brotherlogic/goserver/proto"
	pb "github.com/brotherlogic/lock/proto"
)

const (
	KEY = "github.com/brotherlogic/lock/locks"
)

var (
	numlocks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "lock_numlocks",
		Help: "The number of locks held and stored",
	})
	lockAcq = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "lock_lockAcq",
		Help: "The number of locks held and stored",
	}, []string{"type"})
	lockRel = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "lock_lockRel",
		Help: "The number of locks held and stored",
	}, []string{"type"})
)

func (s *Server) generateLockKey() string {
	return fmt.Sprintf("%v-%v", s.Registry.Identifier, time.Now().UnixNano())
}

func (s *Server) AcquireLock(ctx context.Context, req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	lockAcq.With(prometheus.Labels{"type": fmt.Sprintf("%v", s.LeadState)}).Inc()
	switch s.LeadState {
	case gpb.LeadState_FOLLOWER:
		conn, err := s.FDial(fmt.Sprintf("%v:%v", s.CurrentLead, s.Registry.Port))
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		client := pb.NewLockServiceClient(conn)
		return client.AcquireLock(ctx, req)
	case gpb.LeadState_ELECTING:
		return nil, fmt.Errorf("Currently electing a leader")
	}

	conn, err := s.FDialServer(ctx, "dstore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

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
		err = proto.Unmarshal(rresp.GetValue().GetValue(), locks)
		if err != nil {
			return nil, err
		}
	}

	s.Log(fmt.Sprintf("CONVERSION: %v -> %v", err, locks))

	defer func() {
		numlocks.Set(float64(len(locks.GetLocks())))
	}()

	lock := &pb.Lock{
		AcquireTime: time.Now().Unix(),
		ReleaseTime: time.Now().Unix() + req.GetLockDurationInSeconds(),
		Key:         req.GetKey(),
		LockKey:     s.generateLockKey(),
	}

	// Check that we don't already have this lock
	var nlocks []*pb.Lock
	for _, exLock := range locks.GetLocks() {
		if exLock.GetKey() == req.GetKey() {
			if exLock.GetReleaseTime() > lock.GetAcquireTime() {
				return nil, status.Errorf(codes.AlreadyExists, "This key is locked by %v until %v", exLock.GetLockKey(), time.Unix(exLock.GetReleaseTime(), 0))
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
	lockRel.With(prometheus.Labels{"type": fmt.Sprintf("%v", s.LeadState)}).Inc()
	switch s.LeadState {
	case gpb.LeadState_FOLLOWER:
		conn, err := s.FDial(fmt.Sprintf("%v:%v", s.CurrentLead, s.Registry.Port))
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		client := pb.NewLockServiceClient(conn)
		return client.ReleaseLock(ctx, req)
	case gpb.LeadState_ELECTING:
		return nil, fmt.Errorf("Currently electing a leader")
	}

	conn, err := s.FDialServer(ctx, "dstore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := dspb.NewDStoreServiceClient(conn)
	rresp, err := client.Read(ctx, &dspb.ReadRequest{Key: KEY})
	if err != nil {
		return nil, err
	}

	locks := &pb.Locks{}
	defer func() {
		numlocks.Set(float64(len(locks.GetLocks())))
	}()

	err = proto.Unmarshal(rresp.GetValue().GetValue(), locks)
	if err != nil {
		return nil, err
	}

	changed := false
	var nlocks []*pb.Lock
	for _, exLock := range locks.GetLocks() {
		if exLock.GetKey() != req.GetKey() && exLock.GetLockKey() != req.GetLockKey() {
			nlocks = append(nlocks, exLock)
		} else {
			changed = true
		}
	}
	locks.Locks = nlocks

	if changed {
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
	}

	return &pb.ReleaseLockResponse{}, nil
}
