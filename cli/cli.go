package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/brotherlogic/goserver/utils"
	"google.golang.org/grpc"

	pb "github.com/brotherlogic/filecopier/proto"

	//Needed to pull in gzip encoding init
	_ "google.golang.org/grpc/encoding/gzip"
)

func main() {
	host, port, err := utils.Resolve("filecopier")
	if err != nil {
		log.Fatalf("Unable to reach copier: %v", err)
	}
	conn, err := grpc.Dial(host+":"+strconv.Itoa(int(port)), grpc.WithInsecure())
	defer conn.Close()

	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}

	client := pb.NewFileCopierServiceClient(conn)
	ctx, cancel := utils.BuildContext("filecopier-cli", "filecopier")
	defer cancel()

	if os.Args[1] == "list" {
		resp, err := client.Accepts(ctx, &pb.AcceptsRequest{})
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
		if len(resp.Server) == 0 {
			fmt.Printf("Server accepts nothing!\n")
		}

		for _, server := range resp.Server {
			fmt.Printf("Accepts: '%v'\n", server)
		}
	} else {
		resp, err := client.Copy(ctx, &pb.CopyRequest{InputFile: os.Args[1], InputServer: os.Args[2], OutputFile: os.Args[3], OutputServer: os.Args[4]})

		fmt.Printf("%v and %v\n", resp, err)
	}
}
