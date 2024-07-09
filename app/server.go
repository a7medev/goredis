package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/a7medev/goredis/app/resp"
)

func main() {
	ln, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		log.Fatalln("Failed to bind to port 6379")
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalln("Error accepting connection: ", err.Error())
		}

		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)

		if err == io.EOF {
			fmt.Println("Bye!")
			break
		}

		if err != nil {
			log.Fatalln("Error reading data from connection:", err.Error())
		}

		p := resp.NewParser(buf, n)

		p.NextType()
		p.NextInteger()

		if err != nil {
			log.Fatalln("Error parsing command:", err.Error())
		}

		p.NextType()
		cmd, err := p.NextBulkString()

		if err != nil {
			log.Fatalln("Error parsing command:", err.Error())
		}

		cmd = strings.ToUpper(cmd)

		switch cmd {
		case "PING":
			if _, err := io.Copy(conn, strings.NewReader("+PONG\r\n")); err != nil {
				log.Fatalln("Error sending PONG to the request", err.Error())
			}
		case "ECHO":
			p.NextType()
			msg, err := p.NextBulkString()

			if err != nil {
				log.Fatalln("Error parsing command: ", err.Error())
			}

			if _, err := io.Copy(conn, strings.NewReader(fmt.Sprintf("$%v\r\n%v\r\n", len(msg), msg))); err != nil {
				log.Fatalln("Error sending ECHO to the request", err.Error())
			}
		default:
			fmt.Println("Unknown command", cmd)
		}
	}
}
