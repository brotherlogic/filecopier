package main

import "fmt"

func makeCopyString(server, file string) string {
	if len(server) == 0 {
		return file
	}

	return fmt.Sprintf("%v@%v", server, file)
}
