package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"github.com/brotherlogic/goserver"
	"github.com/brotherlogic/goserver/utils"
	"github.com/brotherlogic/keystore/client"
	"google.golang.org/grpc"

	pbd "github.com/brotherlogic/discovery/proto"
	pb "github.com/brotherlogic/filecopier/proto"
	pbg "github.com/brotherlogic/goserver/proto"
)

type writer interface {
	writeKeys() error
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
}

// Init builds the server
func Init() *Server {
	s := &Server{GoServer: &goserver.GoServer{}}
	s.keys = make(map[string]string)
	s.command = "/usr/bin/scp"
	return s
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {

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

	server.RegisterServer("filecopier", false)
	fmt.Printf("%v\n", server.Serve())
}
