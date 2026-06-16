package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/raft-kv/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("addr", "localhost:8001", "The server address in the format of host:port")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := args[0]

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	switch cmd {
	case "put":
		if len(args) < 3 {
			log.Fatalf("Error: 'put' command requires a value. Usage: put <key> <value>")
		}
		key := args[1]
		val := args[2]
		c := pb.NewKVStoreClient(conn)
		res, err := c.Put(ctx, &pb.PutRequest{Key: key, Value: val})
		if err != nil {
			log.Fatalf("could not put: %v", err)
		}
		fmt.Printf("Put Success: %v, Message: %s\n", res.Success, res.Message)

	case "get":
		key := args[1]
		c := pb.NewKVStoreClient(conn)
		res, err := c.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			log.Fatalf("could not get: %v", err)
		}
		if res.Found {
			fmt.Printf("Get Found: true, Value: %s\n", res.Value)
		} else {
			fmt.Printf("Get Found: false, Message: %s\n", res.Message)
		}

	case "delete":
		key := args[1]
		c := pb.NewKVStoreClient(conn)
		res, err := c.Delete(ctx, &pb.DeleteRequest{Key: key})
		if err != nil {
			log.Fatalf("could not delete: %v", err)
		}
		fmt.Printf("Delete Success: %v, Message: %s\n", res.Success, res.Message)

	case "requestvote":
		// Usage: requestvote <candidateId> <term>
		if len(args) < 3 {
			log.Fatalf("Error: 'requestvote' command requires candidateId and term. Usage: requestvote <candidateId> <term>")
		}
		candidateId, err := strconv.Atoi(args[1])
		if err != nil {
			log.Fatalf("invalid candidateId: %v", err)
		}
		term, err := strconv.Atoi(args[2])
		if err != nil {
			log.Fatalf("invalid term: %v", err)
		}
		c := pb.NewRaftServiceClient(conn)
		res, err := c.RequestVote(ctx, &pb.RequestVoteArgs{
			CandidateId: int32(candidateId),
			Term:        int32(term),
		})
		if err != nil {
			log.Fatalf("could not call RequestVote: %v", err)
		}
		fmt.Printf("RequestVote Reply - Term: %d, VoteGranted: %v\n", res.Term, res.VoteGranted)

	case "appendentries":
		// Usage: appendentries <leaderId> <term>
		if len(args) < 3 {
			log.Fatalf("Error: 'appendentries' command requires leaderId and term. Usage: appendentries <leaderId> <term>")
		}
		leaderId, err := strconv.Atoi(args[1])
		if err != nil {
			log.Fatalf("invalid leaderId: %v", err)
		}
		term, err := strconv.Atoi(args[2])
		if err != nil {
			log.Fatalf("invalid term: %v", err)
		}
		c := pb.NewRaftServiceClient(conn)
		res, err := c.AppendEntries(ctx, &pb.AppendEntriesArgs{
			LeaderId: int32(leaderId),
			Term:     int32(term),
		})
		if err != nil {
			log.Fatalf("could not call AppendEntries: %v", err)
		}
		fmt.Printf("AppendEntries Reply - Term: %d, Success: %v\n", res.Term, res.Success)

	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  client [options] put <key> <value>")
	fmt.Println("  client [options] get <key>")
	fmt.Println("  client [options] delete <key>")
	fmt.Println("  client [options] requestvote <candidateId> <term>")
	fmt.Println("  client [options] appendentries <leaderId> <term>")
	fmt.Println("Options:")
	flag.PrintDefaults()
}
