package main

import (
	"fmt"
	"os/exec"
	"time"

	"golang.org/x/net/context"

	pb "github.com/brotherlogic/filecopier/proto"
)

// ReceiveKey takes a key and adds it
func (s *Server) ReceiveKey(ctx context.Context, in *pb.KeyRequest) (*pb.KeyResponse, error) {
	if val, ok := s.keys[in.Server]; ok {
		if val == in.Key {
			return &pb.KeyResponse{}, nil
		}
	}

	s.keys[in.Server] = in.Key
	err := s.writer.writeKeys(s.keys)

	return &pb.KeyResponse{}, err
}

// Accepts pulls in a key
func (s *Server) Accepts(ctx context.Context, in *pb.AcceptsRequest) (*pb.AcceptsResponse, error) {
	resp := &pb.AcceptsResponse{}
	for key := range s.keys {
		resp.Server = append(resp.Server, key)
	}
	return resp, nil
}

// Copy copies over a key
func (s *Server) Copy(ctx context.Context, in *pb.CopyRequest) (*pb.CopyResponse, error) {
	err := s.checker.check(in.InputServer)
	if err != nil {
		return &pb.CopyResponse{}, fmt.Errorf("Input %v is unable to handle this request: %v", in.InputServer, err)
	}

	err = s.checker.check(in.OutputServer)
	if err != nil {
		return &pb.CopyResponse{}, fmt.Errorf("Output %v is unable to handle this request: %v", in.OutputServer, err)
	}

	command := exec.Command(s.command, makeCopyString(in.InputServer, in.InputFile), makeCopyString(in.OutputServer, in.OutputFile))
	t := time.Now()
	err = command.Start()
	if err != nil {
		return nil, err
	}
	err = command.Wait()
	if err != nil {
		return nil, err
	}

	return &pb.CopyResponse{MillisToCopy: time.Now().Sub(t).Nanoseconds() / 1000000}, nil
}
