package main

import (
	"bufio"
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
	s.Log(fmt.Sprintf("COPY: %v, %v to %v, %v", in.InputServer, in.InputFile, in.OutputServer, in.OutputFile))

	err := s.checker.check(in.InputServer)
	if err != nil {
		return &pb.CopyResponse{}, fmt.Errorf("Input %v is unable to handle this request: %v", in.InputServer, err)
	}

	err = s.checker.check(in.OutputServer)
	if err != nil {
		return &pb.CopyResponse{}, fmt.Errorf("Output %v is unable to handle this request: %v", in.OutputServer, err)
	}

	copyIn := makeCopyString(in.InputServer, in.InputFile)
	copyOut := makeCopyString(in.OutputServer, in.OutputFile)
	command := exec.Command(s.command, "-3", "-o", "StrictHostKeyChecking=no", copyIn, copyOut)

	output := ""
	out, err := command.StderrPipe()
	if err == nil && out != nil {
		scanner := bufio.NewScanner(out)
		go func() {
			for scanner != nil && scanner.Scan() {
				output += scanner.Text()
			}
			out.Close()
		}()

	}
	output2 := ""
	out2, err2 := command.StdoutPipe()
	if err2 == nil && out2 != nil {
		scanner := bufio.NewScanner(out2)
		go func() {
			for scanner != nil && scanner.Scan() {
				output2 += scanner.Text()
			}
			out2.Close()
		}()

	}

	t := time.Now()
	err = command.Start()
	if err != nil {
		return nil, fmt.Errorf("Error running copy: %v, %v -> %v (%v)", copyIn, copyOut, err, output)
	}
	err = command.Wait()
	s.Log(fmt.Sprintf("OUTPUT = %v, %v", output, output2))
	if err != nil {
		return nil, fmt.Errorf("Error waiting on copy: %v, %v -> %v (%v)", copyIn, copyOut, err, output)
	}

	s.Log(fmt.Sprintf("OUTPUT = %v", output))
	return &pb.CopyResponse{MillisToCopy: time.Now().Sub(t).Nanoseconds() / 1000000}, nil
}
