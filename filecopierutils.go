package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func makeCopyString(server, file string) string {
	if len(server) == 0 {
		return file
	}

	return fmt.Sprintf("%v@%v", server, file)
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
