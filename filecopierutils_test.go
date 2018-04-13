package main

import "testing"

func TestServerExpression(t *testing.T) {
	str := makeCopyString("server", "file")
	if str != "server@file" {
		t.Errorf("Bad copy string: %v", str)
	}
}
