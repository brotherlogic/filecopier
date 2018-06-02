package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	pb "github.com/brotherlogic/filecopier/proto"
)

type testChecker struct {
	failServer string
}

func (t *testChecker) check(server string) bool {
	if t.failServer == "" || server != t.failServer {
		return true
	}

	return false
}

type testWriter struct{}

func (t *testWriter) writeKeys(map[string]string) error {
	return nil
}

func InitTestServer() *Server {
	s := Init()
	s.writer = &testWriter{}
	s.checker = &testChecker{}
	return s
}

func TestReceiveKey(t *testing.T) {
	s := InitTestServer()
	_, err := s.ReceiveKey(context.Background(), &pb.KeyRequest{Key: "blah", Server: "TestServer"})
	if err != nil {
		t.Errorf("Error writing key: %v", err)
	}

	a, err := s.Accepts(context.Background(), &pb.AcceptsRequest{})
	if err != nil {
		t.Errorf("Error getting accepts list: %v", err)
	}

	found := false
	for _, val := range a.Server {
		if val == "TestServer" {
			found = true
		}
	}

	if !found {
		t.Errorf("Server has not been found: %v", a)
	}
}

func TestReceiveKeyTwice(t *testing.T) {
	s := InitTestServer()
	_, err := s.ReceiveKey(context.Background(), &pb.KeyRequest{Key: "blah", Server: "TestServer"})
	_, err = s.ReceiveKey(context.Background(), &pb.KeyRequest{Key: "blah", Server: "TestServer"})
	if err != nil {
		t.Errorf("Error writing key: %v", err)
	}

	a, err := s.Accepts(context.Background(), &pb.AcceptsRequest{})
	if err != nil {
		t.Errorf("Error getting accepts list: %v", err)
	}

	found := false
	for _, val := range a.Server {
		if val == "TestServer" {
			found = true
		}
	}

	if !found {
		t.Errorf("Server has not been found: %v", a)
	}
}

func TestCopy(t *testing.T) {
	s := InitTestServer()
	os.Remove("test.txt")
	os.Remove("testout.txt")
	d := []byte("testing")
	ioutil.WriteFile("test.txt", d, 0644)
	dir, _ := os.Getwd()
	_, err := s.Copy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err != nil {
		t.Errorf("Error in copying file: %v", err)
	}

	dOut, err := ioutil.ReadFile("testout.txt")
	if err != nil {
		t.Fatalf("Error reading copied file: %v", err)
	}
	for i := range dOut {
		if d[i] != dOut[i] {
			t.Errorf("Mismatch between files %v and %v", d, dOut)
		}
	}
}

func TestCopyFailCopy(t *testing.T) {
	s := InitTestServer()
	os.Remove("test.txt")
	os.Remove("testout.txt")
	d := []byte("testing")
	ioutil.WriteFile("test.txt", d, 0644)
	dir, _ := os.Getwd()
	_, err := s.Copy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test22.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err == nil {
		t.Errorf("Bad copy did not fail")
	}
}

func TestCopyFailCommand(t *testing.T) {
	s := InitTestServer()
	os.Remove("test.txt")
	os.Remove("testout.txt")
	d := []byte("testing")
	ioutil.WriteFile("test.txt", d, 0644)
	dir, _ := os.Getwd()
	s.command = "blah"
	_, err := s.Copy(context.Background(), &pb.CopyRequest{InputFile: fmt.Sprintf("%v/test.txt", dir), OutputFile: fmt.Sprintf("%v/testout.txt", dir)})

	if err == nil {
		t.Errorf("Bad copy did not fail")
	}
}

func TestCopyFailOutput(t *testing.T) {
	s := InitTestServer()
	s.checker = &testChecker{failServer: "output"}
	_, err := s.Copy(context.Background(), &pb.CopyRequest{InputFile: "test.txt", OutputFile: "testout.txt", OutputServer: "output"})

	if err == nil {
		t.Errorf("No error in copying file: %v", err)
	}
}

func TestCopyFailInput(t *testing.T) {
	s := InitTestServer()
	s.checker = &testChecker{failServer: "input"}
	_, err := s.Copy(context.Background(), &pb.CopyRequest{InputFile: "test.txt", OutputFile: "testout.txt", InputServer: "input"})

	if err == nil {
		t.Errorf("No error in copying file: %v", err)
	}
}
