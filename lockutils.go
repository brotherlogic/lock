package main

import (
	"time"

	"golang.org/x/net/context"
)

func (s *Server) runComputation(ctx context.Context) error {
	t := time.Now()
	sum := 0
	for i := 0; i < 10000; i++ {
		sum += i
	}
	return nil
}
