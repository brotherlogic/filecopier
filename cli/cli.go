package main

import (
	"fmt"
	"log"
	"os"

	"github.com/brotherlogic/goserver/utils"
	"google.golang.org/grpc"

	pb "github.com/brotherlogic/filecopier/proto"

	//Needed to pull in gzip encoding init
	_ "google.golang.org/grpc/encoding/gzip"
)

func main() {
	conn, err := grpc.Dial("runner:57704", grpc.WithInsecure())
	defer conn.Close()

	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}

	client := pb.NewFileCopierServiceClient(conn)
	ctx, cancel := utils.BuildContext("filecopier-cli", "filecopier")
	defer cancel()

	if os.Args[1] == "list" {
		resp, err := client.Accepts(ctx, &pb.AcceptsRequest{Server: "newrunner"})
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
		if len(resp.Server) == 0 {
			fmt.Printf("Server accepts nothing %v!\n", resp)
		}

		for _, server := range resp.Server {
			fmt.Printf("Accepts: '%v'\n", server)
		}
	} else {
		q := &pb.CopyRequest{InputFile: os.Args[1], InputServer: os.Args[2], OutputFile: os.Args[3], OutputServer: os.Args[4]}
		resp, err := client.QueueCopy(ctx, q)

		fmt.Printf("%v -> %v and %v\n", q, resp, err)
	}
}
