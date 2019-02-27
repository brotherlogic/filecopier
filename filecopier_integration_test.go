package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	pb "github.com/brotherlogic/filecopier/proto"
)

func TestQueuedCopy(t *testing.T) {
	s := InitTestServer()

	os.Remove("test.txt")
	os.Remove("testout.txt")
	d := []byte("testing")
	ioutil.WriteFile("test.txt", d, 0644)

	dir, _ := os.Getwd()

	_, err := s.QueueCopy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err == nil {
		t.Errorf("Should have failed")
	}
}
