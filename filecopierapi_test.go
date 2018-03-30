package main

import (
	"context"
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

func (t *testWriter) writeKeys() error {
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
	_, err := s.Copy(context.Background(), &pb.CopyRequest{InputFile: "test.txt", OutputFile: "testout.txt"})

	if err != nil {
		t.Errorf("Error in copying file: %v", err)
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
