package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"github.com/brotherlogic/goserver"
	"github.com/brotherlogic/goserver/utils"
	"github.com/brotherlogic/keystore/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pbd "github.com/brotherlogic/discovery/proto"
	pb "github.com/brotherlogic/filecopier/proto"
	pbg "github.com/brotherlogic/goserver/proto"
)

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
	check(server string) bool
}

type prodChecker struct {
	server string
}

func (p *prodChecker) check(server string) bool {
	conn, err := grpc.Dial(utils.Discover, grpc.WithInsecure())
	if err == nil {
		defer conn.Close()
		client := pbd.NewDiscoveryServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		list, err := client.ListAllServices(ctx, &pbd.ListRequest{})
		if err == nil {
			for _, cl := range list.Services.GetServices() {
				if cl.GetIdentifier() == server {
					conn, err := grpc.Dial(cl.GetIp()+":"+strconv.Itoa(int(cl.GetPort())), grpc.WithInsecure())
					if err == nil {
						defer conn.Close()
						client := pb.NewFileCopierServiceClient(conn)
						accepts, err := client.Accepts(ctx, &pb.AcceptsRequest{})
						if err == nil {
							for _, a := range accepts.Server {
								if a == p.server {
									return true
								}
							}
						}
					}
				}
			}
		}
	}

	return false
}

//Server main server type
type Server struct {
	*goserver.GoServer
	keys    map[string]string
	checker checker
	writer  writer
	command string
	mykey   string
}

// Init builds the server
func Init() *Server {
	s := &Server{
		&goserver.GoServer{},
		make(map[string]string),
		&prodChecker{},
		&prodWriter{},
		"/usr/bin/scp",
		"madeup",
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
	log.Fatalf("WHA: %v -> %v", keys, s.GoServer.Registry.Identifier)
	s.mykey = keys[s.GoServer.Registry.Identifier]
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {
	pb.RegisterFileCopierServiceServer(server, s)
}

// ReportHealth alerts if we're not healthy
func (s *Server) ReportHealth() bool {
	return true
}

// Mote promotes/demotes this server
func (s *Server) Mote(master bool) error {
	return nil
}

// GetState gets the state of the server
func (s *Server) GetState() []*pbg.State {
	return []*pbg.State{}
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
	server.RegisterServer("filecopier", false)
	server.RegisterRepeatingTask(server.shareKeys, time.Hour)
	fmt.Printf("%v\n", server.Serve())
}
