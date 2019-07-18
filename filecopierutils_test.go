package main

import (
	"testing"

	pb "github.com/brotherlogic/filecopier/proto"
	"golang.org/x/net/context"
)

func TestServerExpression(t *testing.T) {
	str := makeCopyString("server", "file")
	if str != "server:file" {
		t.Errorf("Bad copy string: %v", str)
	}
}
func TestBadRead(t *testing.T) {
	_, err := readKeys("madeup/blah.txt")
	if err == nil {
		t.Errorf("Bad read did not fail: %v", err)
	}
}

func TestBadWrite(t *testing.T) {
	err := writeKeys("madeup/blah.txt", make(map[string]string))
	if err == nil {
		t.Errorf("Bad write did not fail: %v", err)
	}
}

func TestReadKeys(t *testing.T) {
	keys, err := readKeys("testdata/testf.txt")
	if err != nil {
		t.Fatalf("Unable to load keys: %v", err)
	}
	val, ok := keys["tasklist"]
	if !ok {
		t.Fatalf("Unable to find key for tasklist: %v", keys)
	}

	if val != "AAAAB3NzaC1yc2EAAAADAQABAAABAQC0ME/rBV/P73sMwapKxQh4hVujgSK8XeWpyLLwSliEnrLmkGREViFMaTGMFkcRdmOPdaxsFe0QWQF+7HshorMexGewfNP/g9+jy433slBF4GkQtvrTMNhi2rQATyIo/Efvhb5QRPSmV5TaC8xjxi/h5JB4OpvNMQ9HKtGj34mohNftCwfai46P0s8t3TbUgSIpXAAwi8bQwEENuNl9DlllCpMQT8ZIcux5DITk7LR74/FoQaugn30oI7EbtlJu5DXYqUfQuX6t2WFTpoIEcgBxSAz97jOUEPP4JkEQ8MFWpp8ibumma+p0mR9ooAiwHkZF9+qLolcMn0hHsB8eXkIr" {
		t.Errorf("Keys do not match: %v", val)
	}
}

func TestWriteKeys(t *testing.T) {
	keys, err := readKeys("testdata/testf.txt")
	if err != nil {
		t.Fatalf("Unable to load keys: %v", err)
	}
	err = writeKeys("testdata/testg.txt", keys)
	if err != nil {
		t.Fatalf("Unable to write keys: %v", err)
	}

	keys, err = readKeys("testdata/testg.txt")
	if err != nil {
		t.Fatalf("Unable to read keys: %v", err)
	}
	val, ok := keys["tasklist"]
	if !ok {
		t.Fatalf("Unable to find key for tasklist: %v", keys)
	}

	if val != "AAAAB3NzaC1yc2EAAAADAQABAAABAQC0ME/rBV/P73sMwapKxQh4hVujgSK8XeWpyLLwSliEnrLmkGREViFMaTGMFkcRdmOPdaxsFe0QWQF+7HshorMexGewfNP/g9+jy433slBF4GkQtvrTMNhi2rQATyIo/Efvhb5QRPSmV5TaC8xjxi/h5JB4OpvNMQ9HKtGj34mohNftCwfai46P0s8t3TbUgSIpXAAwi8bQwEENuNl9DlllCpMQT8ZIcux5DITk7LR74/FoQaugn30oI7EbtlJu5DXYqUfQuX6t2WFTpoIEcgBxSAz97jOUEPP4JkEQ8MFWpp8ibumma+p0mR9ooAiwHkZF9+qLolcMn0hHsB8eXkIr" {
		t.Errorf("Keys do not match: %v", val)
	}
}

func TestRunEmptyQueue(t *testing.T) {
	s := InitTestServer()
	err := s.runQueue(context.Background())
	if err != nil {
		t.Errorf("Error running queue: %v", err)
	}
}

func TestSortQueue(t *testing.T) {
	s := InitTestServer()
	s.queue = append(s.queue, &queueEntry{resp: &pb.CopyResponse{Priority: 10}})
	s.queue = append(s.queue, &queueEntry{resp: &pb.CopyResponse{Priority: 100}})

	s.sortQueue()

	if s.queue[0].resp.Priority == 100 {
		t.Errorf("Queue is missorted")
	}
}

func TestSortQueueV1(t *testing.T) {
	s := InitTestServer()
	s.queue = append(s.queue, &queueEntry{resp: &pb.CopyResponse{Status: pb.CopyStatus_COMPLETE}})
	s.queue = append(s.queue, &queueEntry{resp: &pb.CopyResponse{Status: pb.CopyStatus_IN_QUEUE}})

	s.sortQueue()

	if s.queue[0].resp.Status == pb.CopyStatus_COMPLETE {
		t.Errorf("Queue is missorted")
	}
}

func TestSortQueueV2(t *testing.T) {
	s := InitTestServer()
	s.queue = append(s.queue, &queueEntry{resp: &pb.CopyResponse{Status: pb.CopyStatus_IN_QUEUE}})
	s.queue = append(s.queue, &queueEntry{resp: &pb.CopyResponse{Status: pb.CopyStatus_COMPLETE}})

	s.sortQueue()

	if s.queue[0].resp.Status == pb.CopyStatus_COMPLETE {
		t.Errorf("Queue is missorted")
	}
}
