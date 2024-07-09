package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	ln, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	conn, err := ln.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	if _, err := io.Copy(conn, strings.NewReader("+PONG\r\n")); err != nil {
		fmt.Println("Error sending PONG to the request", err.Error())
		os.Exit(1)
	}
}
