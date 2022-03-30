package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/brotherlogic/goserver"
	"github.com/brotherlogic/goserver/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/brotherlogic/filecopier/proto"
	pbg "github.com/brotherlogic/goserver/proto"
)

var (
	rkeys = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "filecopier_keys",
		Help: "The number of keys",
	})
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
	key    string
	dial   func(ctx context.Context, job, server string) (*grpc.ClientConn, error)
}

func (p *prodChecker) check(server string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, err := p.dial(ctx, "filecopier", server)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewFileCopierServiceClient(conn)
	_, err = client.Accepts(ctx, &pb.AcceptsRequest{Server: p.server, Key: p.key})
	return err
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
	tCopyTime       time.Duration
	queueChan       chan *queueEntry
	current         *pb.CopyRequest
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
		0,
		make(chan *queueEntry, 100),
		nil,
	}

	s.checker = &prodChecker{dial: s.FDialSpecificServer}
	return s
}

func (s *Server) keySetup() error {
	keys, err := readKeys("/home/simon/.ssh/authorized_keys")
	if err == nil {
		s.keys = keys
	}

	rkeys.Set(float64(len(s.keys)))

	keys, err = readKeys("/home/simon/.ssh/id_rsa.pub")
	if err != nil {
		return err
	}

	if len(keys) != 1 {
		return fmt.Errorf("Weird key setup: %v", keys)
	}

	for _, val := range keys {
		s.mykey = val
	}

	s.Log(fmt.Sprintf("Read keys-> %v, my key is %v", s.keys, s.mykey))

	return nil
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
	return []*pbg.State{}
}

func (s *Server) shareKeys(ctx context.Context) error {
	entities, err := s.FFind(ctx, "filecopier")

	if err == nil {
		for _, e := range entities {
			if !strings.HasPrefix(e, s.Registry.GetIdentifier()) {
				conn, err := s.FDial(e)
				defer conn.Close()
				if err == nil {
					client := pb.NewFileCopierServiceClient(conn)
					_, err := client.ReceiveKey(ctx, &pb.KeyRequest{Key: s.mykey, Server: s.GoServer.Registry.Identifier})
					code := status.Convert(err).Code()
					if code != codes.OK && code != codes.Unavailable {
						log.Fatalf("Unable to receive key: %v", err)
					}
				}
			}
		}
	}

	return nil
}

func (s *Server) cleanQueue(ctx context.Context) error {
	newQueue := s.queue
	s.queue = nil

	for _, elem := range newQueue {
		if elem.resp.Status != pb.CopyStatus_COMPLETE || time.Now().Sub(elem.timeAdded) < time.Minute*5 {
			s.queue = append(s.queue, elem)
		}
	}

	return nil
}

var (
	copies = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "filecopier_copies",
		Help: "The number of server requests",
	}, []string{"file", "destination"})
)

func (s *Server) runCopy(in *pb.CopyRequest) error {
	s.current = in
	copies.With(prometheus.Labels{"file": in.InputFile, "destination": in.OutputServer}).Inc()
	stTime := time.Now()
	s.lastCopyTime = time.Now()
	s.lastCopyDetails = fmt.Sprintf("%v from %v to %v (%v)", in.InputFile, in.InputServer, in.OutputServer, in.OutputFile)
	s.ccopies++
	defer s.reduce()

	s.Log(fmt.Sprintf("COPY: %v, %v to %v, %v", in.InputServer, in.InputFile, in.OutputServer, in.OutputFile))
	s.copies++

	err := s.checker.check(in.InputServer)
	if err != nil {
		s.lastError = fmt.Sprintf("IN: %v", err)
		return status.Errorf(status.Convert(err).Code(), "Input %v is unable to handle this request: %v", in.InputServer, err)
	}

	err = s.checker.check(in.OutputServer)
	if err != nil {
		s.lastError = fmt.Sprintf("OUT: %v", err)
		return status.Errorf(status.Convert(err).Code(), "Output %v is unable to handle this request: %v", in.OutputServer, err)
	}

	copyIn := s.makeCopyString(in.InputServer, in.InputFile)
	copyOut := s.makeCopyString(in.OutputServer, in.OutputFile)
	command := exec.Command(s.command, "-p", "-o", "StrictHostKeyChecking=no", copyIn, copyOut)

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

	err = command.Start()
	if err != nil {
		s.lastError = fmt.Sprintf("CS %v", err)
		s.Log(fmt.Sprintf("Error running copy: %v, %v -> %v (%v)", copyIn, copyOut, err, output))
		return fmt.Errorf("Error running copy: %v, %v -> %v (%v)", copyIn, copyOut, err, output)
	}
	err = command.Wait()

	if err != nil {
		s.lastError = fmt.Sprintf("CW %v", err)
		s.Log(fmt.Sprintf("Error waiting on copy: %v, %v -> %v (%v)", copyIn, copyOut, err, output))
		return fmt.Errorf("Error waiting on copy: %v, %v -> %v (%v)", copyIn, copyOut, err, output)
	}

	if len(output) > 0 && !strings.Contains("lost connection", output) {
		if strings.Contains(output, "Permanently added") {
			//Ignore this
		} else if strings.Contains(output, "differs") {
			s.RaiseIssue("Found mismatch error", fmt.Sprintf("[%v] Mismatch Error on copy: %v (%v -> %v)", s.Registry.Identifier, output, copyIn, copyOut))
		} else {
			s.RaiseIssue("Copy Error", fmt.Sprintf("[%v] Error on the copy: %v (%v -> %v)", s.Registry.Identifier, output, copyIn, copyOut))
		}
	}

	s.copyTime = time.Now().Sub(stTime)
	s.tCopyTime += time.Now().Sub(stTime)

	if s.copyTime > time.Hour {
		s.RaiseIssue("Long Copy Time", fmt.Sprintf("Copy from %v to %v took %v", in.InputServer, in.OutputServer, s.copyTime))
	}

	s.lastError = fmt.Sprintf("DONE %v", output)
	s.Log(fmt.Sprintf("Completed %v -> %v with %v in %v", copyIn, copyOut, output, s.copyTime))

	//Perform the callback if needed - this is fire and forget
	if len(in.GetCallback()) > 0 {
		conn, err := s.FDial(in.GetCallback())
		if err == nil {
			defer conn.Close()
			ctx, cancel := utils.ManualContext("filecopier-callback", time.Minute)
			s.CtxLog(ctx, fmt.Sprintf("Callingback: %v", in.GetCallback()))
			client := pb.NewFileCopierCallbackClient(conn)
			client.Callback(ctx, &pb.CallbackRequest{Key: in.GetKey()})
			cancel()
		}
	}

	return nil
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
	server.PrepServer()
	server.Register = server

	err := server.keySetup()

	if err != nil {
		fmt.Printf("Something is wrong with the key setup: %v", err)
		return
	}

	err = server.RegisterServerV2("filecopier", false, true)
	server.DiskLog = true

	if err == nil {
		//Set the server name
		server.checker = &prodChecker{
			key:    server.mykey,
			server: server.Registry.Identifier,
			dial:   server.FDialSpecificServer,
		}

		// Run the queue processor
		go server.runQueue()

		if server.Registry.Identifier == "rdisplay" {
			server.NoProm = true
		}

		fmt.Printf("%v\n", server.Serve())
	}
}
