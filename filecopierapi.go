package main

import (
	"bufio"
	"fmt"
	"os/exec"
	"time"

	pb "github.com/brotherlogic/filecopier/proto"
	"golang.org/x/net/context"
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

func (s *Server) reduce() {
	s.ccopiesMutex.Lock()
	s.ccopies--
	s.ccopiesMutex.Unlock()
}

// QueueCopy copies over a key using a queue
func (s *Server) QueueCopy(ctx context.Context, in *pb.CopyRequest) (*pb.CopyResponse, error) {
	for _, q := range s.queue {
		if in.InputServer == q.req.InputServer && in.OutputServer == q.req.OutputServer &&
			in.InputFile == q.req.InputFile && in.OutputFile == q.req.OutputFile {
			return q.resp, nil
		}
	}

	r := &pb.CopyResponse{Status: pb.CopyStatus_IN_QUEUE, TimeInQueue: time.Now().UnixNano()}
	s.queue = append(s.queue, &queueEntry{req: in, resp: r})
	return r, nil
}

// Copy copies over a key
func (s *Server) Copy(ctx context.Context, in *pb.CopyRequest) (*pb.CopyResponse, error) {
	s.ccopiesMutex.Lock()
	if s.ccopies > 0 {
		s.ccopiesMutex.Unlock()
		return nil, fmt.Errorf("Too many concurrent copies")
	}

	s.ccopies++
	s.ccopiesMutex.Unlock()

	t := time.Now()
	err := s.runCopy(ctx, in)
	defer s.reduce()
	return &pb.CopyResponse{MillisToCopy: time.Now().Sub(t).Nanoseconds() / 1000000}, err
}

func (s *Server) runCopy(ctx context.Context, in *pb.CopyRequest) error {
	stTime := time.Now()
	s.lastCopyDetails = fmt.Sprintf("%v from %v to %v", in.InputFile, in.InputServer, in.OutputServer)
	s.ccopies++
	defer s.reduce()

	s.Log(fmt.Sprintf("COPY: %v, %v to %v, %v", in.InputServer, in.InputFile, in.OutputServer, in.OutputFile))
	s.copies++

	err := s.checker.check(in.InputServer)
	if err != nil {
		s.lastError = fmt.Sprintf("IN: %v", err)
		s.Log(fmt.Sprintf("Failed to check %v", in.InputServer))
		return fmt.Errorf("Input %v is unable to handle this request: %v", in.InputServer, err)
	}

	err = s.checker.check(in.OutputServer)
	if err != nil {
		s.lastError = fmt.Sprintf("OUT: %v", err)
		s.Log(fmt.Sprintf("Failed to check %v", in.OutputServer))
		return fmt.Errorf("Output %v is unable to handle this request: %v", in.OutputServer, err)
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
				s.Log(fmt.Sprintf("SCANERR: %v", output))
			}
			out.Close()
		}()

	}

	err = command.Start()
	if err != nil {
		s.lastError = fmt.Sprintf("CS %v", err)
		return fmt.Errorf("Error running copy: %v, %v -> %v (%v)", copyIn, copyOut, err, output)
	}
	err = command.Wait()

	if err != nil {
		s.lastError = fmt.Sprintf("CW %v", err)
		return fmt.Errorf("Error waiting on copy: %v, %v -> %v (%v)", copyIn, copyOut, err, output)
	}

	s.copyTime = time.Now().Sub(stTime)
	s.lastError = fmt.Sprintf("DONE %v", output)
	return nil
}
