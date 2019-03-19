package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"sync"
	"time"

	pbd "github.com/brotherlogic/discovery/proto"
	pb "github.com/brotherlogic/filecopier/proto"
	"github.com/brotherlogic/goserver"
	pbg "github.com/brotherlogic/goserver/proto"
	"github.com/brotherlogic/goserver/utils"
	"github.com/brotherlogic/keystore/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type queueEntry struct {
	req       *pb.CopyRequest
	resp      *pb.CopyResponse
	timeAdded time.Time
}

type writer interface {
	writeKeys(keys map[string]string) error
}

type prodWriter struct {
	file string
}

func (p *prodWriter) writeKeys(keys map[string]string) error {
	return writeKeys(p.file, keys)
}

type checker interface {
	check(server string) error
}

type prodChecker struct {
	server string
}

func (p *prodChecker) check(server string) error {
	conn, err := grpc.Dial(utils.Discover, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("Error dialing discover: %v", err)
	}

	defer conn.Close()
	client := pbd.NewDiscoveryServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	list, err := client.ListAllServices(ctx, &pbd.ListRequest{})
	if err != nil {
		return fmt.Errorf("Failled to list all services: %v", err)
	}

	for _, cl := range list.Services.GetServices() {
		if cl.GetIdentifier() == server && cl.GetName() == "filecopier" {
			conn, err := grpc.Dial(cl.GetIp()+":"+strconv.Itoa(int(cl.GetPort())), grpc.WithInsecure())
			if err != nil {
				return fmt.Errorf("Failed to dial: %v", err)
			}

			defer conn.Close()
			client := pb.NewFileCopierServiceClient(conn)
			accepts, err := client.Accepts(ctx, &pb.AcceptsRequest{})
			if err != nil {
				fmt.Errorf("Failed on accept: %v", err)
				return err
			}

			for _, a := range accepts.Server {
				if a == p.server {
					return nil
				}
			}
			return fmt.Errorf("Match not found (%v), and %v", accepts, p.server)
		}

	}

	return fmt.Errorf("Server %v was not found", server)
}

//Server main server type
type Server struct {
	*goserver.GoServer
	keys            map[string]string
	checker         checker
	writer          writer
	command         string
	mykey           string
	copies          int64
	lastError       string
	ccopies         int64
	ccopiesMutex    *sync.Mutex
	lastCopyTime    time.Time
	lastCopyDetails string
	copyTime        time.Duration
	queue           []*queueEntry
	currout         string
}

// Init builds the server
func Init() *Server {
	s := &Server{
		&goserver.GoServer{},
		make(map[string]string),
		&prodChecker{},
		&prodWriter{file: "/home/simon/.ssh/authorized_keys"},
		"/usr/bin/scp",
		"madeup",
		int64(0),
		"",
		int64(0),
		&sync.Mutex{},
		time.Unix(1, 0),
		"",
		0,
		make([]*queueEntry, 0),
		"",
	}
	return s
}

func (s *Server) keySetup() {
	keys, err := readKeys("/home/simon/.ssh/authorized_keys")
	if err == nil {
		s.keys = keys
	}

	keys, err = readKeys("/home/simon/.ssh/id_rsa.pub")
	if err != nil {
		panic(err)
	}

	if len(keys) != 1 {
		log.Fatalf("Weird key setup: %v", keys)
	}

	for _, val := range keys {
		s.mykey = val
	}
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {
	pb.RegisterFileCopierServiceServer(server, s)
}

// ReportHealth alerts if we're not healthy
func (s *Server) ReportHealth() bool {
	return true
}

//Shutdown the server
func (s *Server) Shutdown(ctx context.Context) error {
	return nil
}

// Mote promotes/demotes this server
func (s *Server) Mote(ctx context.Context, master bool) error {
	return nil
}

// GetState gets the state of the server
func (s *Server) GetState() []*pbg.State {
	s.ccopiesMutex.Lock()
	defer s.ccopiesMutex.Unlock()

	inQueue := int64(0)
	for _, q := range s.queue {
		if q.resp.Status == pb.CopyStatus_IN_QUEUE {
			inQueue++
		}
	}
	return []*pbg.State{
		&pbg.State{Key: "keys", Value: int64(len(s.keys))},
		&pbg.State{Key: "copies", Value: s.copies},
		&pbg.State{Key: "con_copies", Value: s.ccopies},
		&pbg.State{Key: "last_copy", Text: s.lastCopyDetails},
		&pbg.State{Key: "queued", Value: int64(len(s.queue))},
		&pbg.State{Key: "waiting", Value: inQueue},
		&pbg.State{Key: "copy_time", TimeDuration: s.copyTime.Nanoseconds()},
		&pbg.State{Key: "current_output", Text: s.currout},
	}
}

func (s *Server) shareKeys(ctx context.Context) {
	entities, err := utils.ResolveAll("filecopier")

	if err == nil {
		for _, e := range entities {
			conn, err := grpc.Dial(e.Ip+":"+strconv.Itoa(int(e.Port)), grpc.WithInsecure())
			defer conn.Close()
			if err == nil {
				client := pb.NewFileCopierServiceClient(conn)
				client.ReceiveKey(ctx, &pb.KeyRequest{Key: s.mykey, Server: s.GoServer.Registry.Identifier})
			}
		}
	}
}

func (s *Server) cleanQueue(ctx context.Context) {
	newQueue := s.queue
	s.queue = nil

	for _, elem := range newQueue {
		if time.Now().Sub(elem.timeAdded) < time.Minute*5 {
			s.queue = append(s.queue, elem)
		}
	}
}

func main() {
	var quiet = flag.Bool("quiet", false, "Show all output")
	flag.Parse()

	//Turn off logging
	if *quiet {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	server := Init()
	server.GoServer.KSclient = *keystoreclient.GetClient(server.GetIP)
	server.PrepServer()
	server.Register = server

	server.keySetup()
	err := server.RegisterServer("filecopier", false)
	if err == nil {
		server.RegisterRepeatingTaskNonMaster(server.shareKeys, "share_keys", time.Hour)
		server.RegisterRepeatingTask(server.runQueue, "run_queue", time.Second)
		server.RegisterRepeatingTask(server.cleanQueue, "clean_queue", time.Minute)

		//Set the server name
		server.checker = &prodChecker{server: server.Registry.Identifier}

		fmt.Printf("%v\n", server.Serve())
	} else {
		log.Fatalf("Unable to register: %v", err)
	}
}
