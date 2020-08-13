package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	pb "github.com/brotherlogic/filecopier/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/status"
)

func (s *Server) sortQueue(ctx context.Context) {
	for _, q := range s.queue {
		if q == nil {
			s.RaiseIssue("Empty Element in QUeue", fmt.Sprintf("%v", s.queue))
			return
		}
	}

	sort.SliceStable(s.queue, func(i, j int) bool {
		if s.queue[i].resp.GetStatus() == pb.CopyStatus_IN_QUEUE && s.queue[j].resp.GetStatus() != pb.CopyStatus_IN_QUEUE {
			return true
		}
		if s.queue[i].resp.Status != pb.CopyStatus_IN_QUEUE && s.queue[j].resp.Status == pb.CopyStatus_IN_QUEUE {
			return false
		}
		return s.queue[i].resp.Priority < s.queue[j].resp.Priority
	})

}

func (s *Server) runQueue() {
	for entry := range s.queueChan {
		entry.resp.Status = pb.CopyStatus_IN_PROGRESS
		err := s.runCopy(entry.req)
		if err != nil {
			entry.resp.Error = fmt.Sprintf("%v", err)
			entry.resp.ErrorCode = int32(status.Convert(err).Code())
		}
		entry.resp.Status = pb.CopyStatus_COMPLETE
	}
}

func (s *Server) makeCopyString(server, file string) string {
	if len(server) == 0 || server == s.Registry.GetIdentifier() {
		return file
	}

	return fmt.Sprintf("%v:%v", server, file)
}

func readKeys(filename string) (map[string]string, error) {
	keys := make(map[string]string)

	file, err := os.Open(filename)
	if err != nil {
		return keys, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		pieces := strings.Fields(scanner.Text())
		bits := strings.Split(pieces[2], "@")
		keys[bits[1]] = pieces[1]
	}

	return keys, nil
}

func writeKeys(file string, keys map[string]string) error {
	f, err := os.Create(file)
	defer f.Close()

	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	for key, value := range keys {
		w.WriteString(fmt.Sprintf("ssh-rsa %v simon@%v\n", value, key))
	}
	w.Flush()

	return nil

}
