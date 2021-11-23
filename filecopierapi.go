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
			return &pb.KeyResponse{Mykey: s.mykey}, nil
		}
	}

	s.keys[in.Server] = in.Key
	rkeys.Set(float64(len(s.keys)))
	err := s.writer.writeKeys(s.keys)

	return &pb.KeyResponse{Mykey: s.mykey}, err
}

// Accepts pulls in a key
func (s *Server) Accepts(ctx context.Context, in *pb.AcceptsRequest) (*pb.AcceptsResponse, error) {
	for key, keyv := range s.keys {
		if key == in.GetServer() && in.GetKey() == keyv {
			return &pb.AcceptsResponse{Type: "found-in-server"}, nil
		}
	}

	conn, err := s.FDialSpecificServer(ctx, "filecopier", in.GetServer())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pb.NewFileCopierServiceClient(conn)
	resp, err := client.ReceiveKey(ctx, &pb.KeyRequest{Key: s.mykey, Server: s.GoServer.Registry.Identifier})
	if err != nil {
		return nil, err
	}

	if len(resp.GetMykey()) == 0 {
		return nil, fmt.Errorf("bad key passed in accepts: %v", resp)
	}

	s.keys[in.Server] = resp.GetMykey()
	rkeys.Set(float64(len(s.keys)))

	return &pb.AcceptsResponse{Type: "key-passed"}, s.writer.writeKeys(s.keys)
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
	var nq []*queueEntry
	for ind, q := range s.queue {
		if in.InputServer == q.req.InputServer && in.OutputServer == q.req.OutputServer &&
			in.InputFile == q.req.InputFile && in.OutputFile == q.req.OutputFile {
			if !in.GetOverride() && q.resp.Status == pb.CopyStatus_COMPLETE {
				q.resp.IndexInQueue = int32(ind)
				var err error
				if len(q.resp.GetError()) > 0 {
					err = status.Errorf(codes.Code(q.resp.GetErrorCode()), "%v", q.resp.GetError())
				}
				s.Log(fmt.Sprintf("Found (%v) in queue: %v -> %v", q.req, ind, q.resp))
				return q.resp, err
			}
		} else {
			nq = append(nq, q)
		}
	}
	s.queue = nq

	r := &pb.CopyResponse{Status: pb.CopyStatus_IN_QUEUE, TimeInQueue: time.Now().UnixNano()}
	entry := &queueEntry{req: in, resp: r, timeAdded: time.Now()}
	queue.With(prometheus.Labels{"file": in.InputFile, "destination": in.OutputServer}).Inc()
	s.queue = append(s.queue, entry)
	s.queueChan <- entry
	s.Log(fmt.Sprintf("Added to queue: %v", len(s.queueChan)))
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
