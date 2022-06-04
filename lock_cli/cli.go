package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brotherlogic/goserver/utils"

	pb "github.com/brotherlogic/lock/proto"

	//Needed to pull in gzip encoding init

	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/resolver"
)

func init() {
	resolver.Register(&utils.DiscoveryClientResolverBuilder{})
}

func main() {
	ctx, cancel := utils.ManualContext("lock-cli", time.Minute)
	defer cancel()

	conn, err := utils.LFDialServer(ctx, "lock")
	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewLockServiceClient(conn)

	switch os.Args[1] {
	case "probe":
		res, err := client.ProbeLock(ctx, &pb.ProbeLockRequest{Key: os.Args[2]})
		fmt.Printf("%v -> %v\n", res, err)
	case "acquire":
		res, err := client.AcquireLock(ctx, &pb.AcquireLockRequest{Key: os.Args[2], LockDurationInSeconds: int64(60 * 5)})
		fmt.Printf("%v -> %v\n", res, err)
	case "release":
		res, err := client.ReleaseLock(ctx, &pb.ReleaseLockRequest{Key: os.Args[2], LockKey: os.Args[3]})
		fmt.Printf("%v -> %v\n", res, err)
	}
}
