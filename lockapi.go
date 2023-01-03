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
	lockActAcq = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lock_act_acq",
		Help: "The number of locks held and stored",
	})
	lockAcq = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "lock_acq",
		Help: "The number of locks held and stored",
	}, []string{"type"})
	lockActRel = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lock_act_rel",
		Help: "The number of locks held and stored",
	})
	lockRel = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "lock_rel",
		Help: "The number of locks held and stored",
	}, []string{"type"})

	lockWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "lock_wait_time",
		Help:    "The latency of server requests",
		Buckets: []float64{.005 * 1000, .01 * 1000, .025 * 1000, .05 * 1000, .1 * 1000, .25 * 1000, .5 * 1000, 1 * 1000, 2.5 * 1000, 5 * 1000, 10 * 1000, 100 * 1000, 1000 * 1000},
	})
)

func (s *Server) generateLockKey() string {
	return fmt.Sprintf("%v-%v", s.Registry.Identifier, time.Now().UnixNano())
}

func (s *Server) ProbeLock(ctx context.Context, req *pb.ProbeLockRequest) (*pb.ProbeLockResponse, error) {
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

	s.CtxLog(ctx, fmt.Sprintf("Read %v with %v", rresp.GetHash(), rresp.GetConsensus()))

	locks := &pb.Locks{}

	//Only unmarshal if we actually read something
	if status.Convert(err).Code() != codes.NotFound {
		err = proto.Unmarshal(rresp.GetValue().GetValue(), locks)
		if err != nil {
			return nil, err
		}
	}

	for _, lock := range locks.GetLocks() {
		if lock.GetKey() == req.GetKey() {
			return &pb.ProbeLockResponse{Lock: lock}, nil
		}
	}

	return nil, status.Errorf(codes.NotFound, "Could not find lock with key: %v", req.GetKey())
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
		return nil, status.Errorf(codes.PermissionDenied, "Currently electing a leader")
	}
	lockActAcq.Inc()

	// Lock for the main acquisition
	t := time.Now()
	s.mlock.Lock()
	defer s.mlock.Unlock()
	lockWait.Observe(float64(time.Since(t).Milliseconds()))

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

	s.CtxLog(ctx, fmt.Sprintf("Read %v with %v", rresp.GetHash(), rresp.GetConsensus()))

	locks := &pb.Locks{}

	//Only unmarshal if we actually read something
	if status.Convert(err).Code() != codes.NotFound {
		err = proto.Unmarshal(rresp.GetValue().GetValue(), locks)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		numlocks.Set(float64(len(locks.GetLocks())))
	}()

	lock := &pb.Lock{
		AcquireTime: time.Now().Unix(),
		ReleaseTime: time.Now().Unix() + req.GetLockDurationInSeconds(),
		Key:         req.GetKey(),
		LockKey:     s.generateLockKey(),
		Purpose:     req.GetPurpose(),
	}

	s.CtxLog(ctx, fmt.Sprintf("created lock: %v", lock))

	// Check that we don't already have this lock
	var nlocks []*pb.Lock
	for _, exLock := range locks.GetLocks() {
		if exLock.GetKey() == req.GetKey() {
			if exLock.GetReleaseTime() > lock.GetAcquireTime() {
				return nil, status.Errorf(codes.AlreadyExists, "This key (%v) is locked by %v until %v: %v", req.GetKey(), exLock.GetLockKey(), time.Unix(exLock.GetReleaseTime(), 0), exLock.GetPurpose())
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

	s.CtxLog(ctx, fmt.Sprintf("Written %v with %v", wresp.GetHash(), wresp.GetConsensus()))

	if err != nil {
		return nil, err
	}

	if wresp.GetConsensus() < 1.0 {
		return nil, status.Errorf(codes.FailedPrecondition, "could not get lock consensus on write (%v)", wresp.GetConsensus())
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
		return nil, status.Errorf(codes.PermissionDenied, "Currently electing a leader")
	}
	lockActRel.Inc()

	// Also lock on release
	s.mlock.Lock()
	defer s.mlock.Unlock()

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
	las := len(locks.GetLocks())
	for _, exLock := range locks.GetLocks() {
		if exLock.GetKey() != req.GetKey() && exLock.GetLockKey() != req.GetLockKey() {
			nlocks = append(nlocks, exLock)
		} else {
			changed = true
		}
	}
	locks.Locks = nlocks
	s.CtxLog(ctx, fmt.Sprintf("For %v -> %v to %v (%v)", req, las, len(locks.GetLocks()), changed))

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
