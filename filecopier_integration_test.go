package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	pb "github.com/brotherlogic/filecopier/proto"
)

func TestQueuedCopy(t *testing.T) {
	s := InitTestServer()

	os.Remove("test.txt")
	os.Remove("testout.txt")
	d := []byte("testing")
	ioutil.WriteFile("test.txt", d, 0644)

	dir, _ := os.Getwd()

	r, err := s.QueueCopy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err != nil {
		t.Fatalf("Error queueing copy: %v", err)
	}

	if r.Status != pb.CopyStatus_IN_QUEUE {
		t.Errorf("Bad status respnse: %v", r.Status)
	}

	time.Sleep(time.Second)
	r2, err := s.QueueCopy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err != nil {
		t.Fatalf("Error queueing copy: %v", err)
	}

	if r2.Status != pb.CopyStatus_IN_QUEUE {
		t.Errorf("Copy has changed status: %v", r2.Status)
	}

	if r2.TimeInQueue != r.TimeInQueue {
		t.Errorf("Not returning the same copy")
	}

	s.runQueue(context.Background())

	r3, err := s.QueueCopy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err != nil {
		t.Fatalf("Error queueing copy: %v", err)
	}

	if r3.Status != pb.CopyStatus_COMPLETE {
		t.Errorf("Copy has not run: %v", r3)
	}

}

func TestQueuedCopyWithFailure(t *testing.T) {
	s := InitTestServer()

	os.Remove("test.txt")
	os.Remove("testout.txt")
	d := []byte("testing")
	ioutil.WriteFile("test.txt", d, 0644)

	dir, _ := os.Getwd()

	r, err := s.QueueCopy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test222.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err != nil {
		t.Fatalf("Error queueing copy: %v", err)
	}

	if r.Status != pb.CopyStatus_IN_QUEUE {
		t.Errorf("Bad status respnse: %v", r.Status)
	}

	time.Sleep(time.Second)
	r2, err := s.QueueCopy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test222.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err != nil {
		t.Fatalf("Error queueing copy: %v", err)
	}

	if r2.Status != pb.CopyStatus_IN_QUEUE {
		t.Errorf("Copy has changed status: %v", r2.Status)
	}

	if r2.TimeInQueue != r.TimeInQueue {
		t.Errorf("Not returning the same copy")
	}

	s.runQueue(context.Background())

	r3, err := s.QueueCopy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test222.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err == nil {
		t.Fatalf("Error queueing copy: %v", err)
	}

	if r3.Status != pb.CopyStatus_COMPLETE {
		t.Errorf("Copy has not run: %v", r3)
	}

	if len(r3.Error) == 0 {
		t.Errorf("Bad copy did not fail")
	}

}
