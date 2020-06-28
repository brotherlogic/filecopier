package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	pb "github.com/brotherlogic/filecopier/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReceiveKey takes a key and adds it
func (s *Server) ReceiveKey(ctx context.Context, in *pb.KeyRequest) (*pb.KeyResponse, error) {
	if val, ok := s.keys[in.Server]; ok {
		if val == in.Key {
			return &pb.KeyResponse{}, nil
		}
	}

	s.keys[in.Server] = in.Key
	rkeys.Set(float64(len(s.keys)))
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
		s.QueueCopy(ctx, &pb.CopyRequest{InputServer: in.InputServer, InputFile: path, OutputServer: in.OutputServer, OutputFile: in.OutputFile + path, Priority: 100})
		return nil
	})
	return &pb.CopyResponse{}, err
}

var (
	queue = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "filecopier_queued",
		Help: "The number of server requests",
	}, []string{"file", "destination"})
)

// QueueCopy copies over a key using a queue
func (s *Server) QueueCopy(ctx context.Context, in *pb.CopyRequest) (*pb.CopyResponse, error) {
	for ind, q := range s.queue {
		if in.InputServer == q.req.InputServer && in.OutputServer == q.req.OutputServer &&
			in.InputFile == q.req.InputFile && in.OutputFile == q.req.OutputFile {
			q.resp.IndexInQueue = int32(ind)
			var err error
			if len(q.resp.GetError()) > 0 {
				err = status.Errorf(codes.Code(q.resp.GetErrorCode()), "%v", q.resp.GetError())
			}
			return q.resp, err
		}
	}

	r := &pb.CopyResponse{Status: pb.CopyStatus_IN_QUEUE, TimeInQueue: time.Now().UnixNano()}
	entry := &queueEntry{req: in, resp: r, timeAdded: time.Now()}
	queue.With(prometheus.Labels{"file": in.InputFile, "destination": in.OutputServer}).Inc()
	s.queue = append(s.queue, entry)
	s.queueChan <- entry
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
	err := s.runCopy(in)
	defer s.reduce()
	return &pb.CopyResponse{MillisToCopy: time.Now().Sub(t).Nanoseconds() / 1000000}, err
}
