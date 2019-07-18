package main

import (
	"fmt"
	"os"
	"path/filepath"
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

// DirCopy copies a directory
func (s *Server) DirCopy(ctx context.Context, in *pb.CopyRequest) (*pb.CopyResponse, error) {
	err := filepath.Walk(in.InputFile, func(path string, info os.FileInfo, walkerr error) error {
		s.Log(fmt.Sprintf("Would copy %v", path))
		s.QueueCopy(ctx, &pb.CopyRequest{InputServer: in.InputServer, InputFile: path, OutputServer: in.OutputServer, OutputFile: in.OutputFile + path, Priority: 100})
		return nil
	})
	return &pb.CopyResponse{}, err
}

// QueueCopy copies over a key using a queue
func (s *Server) QueueCopy(ctx context.Context, in *pb.CopyRequest) (*pb.CopyResponse, error) {
	for ind, q := range s.queue {
		if in.InputServer == q.req.InputServer && in.OutputServer == q.req.OutputServer &&
			in.InputFile == q.req.InputFile && in.OutputFile == q.req.OutputFile {
			q.resp.IndexInQueue = int32(ind)
			return q.resp, nil
		}
	}

	r := &pb.CopyResponse{Status: pb.CopyStatus_IN_QUEUE, TimeInQueue: time.Now().UnixNano()}
	s.queue = append(s.queue, &queueEntry{req: in, resp: r, timeAdded: time.Now()})
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
