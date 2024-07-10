package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/a7medev/goredis/app/resp"
)

var db = make(map[string]string)

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
			pong := resp.NewSimpleString("PONG").Encode()

			if _, err := io.Copy(conn, strings.NewReader(pong)); err != nil {
				log.Fatalln("Error sending PONG to the request", err.Error())
			}
		case "ECHO":
			p.NextType()
			msg, err := p.NextBulkString()

			if err != nil {
				log.Fatalln("Error parsing command: ", err.Error())
			}

			result := resp.NewBulkString(msg).Encode()

			if _, err := io.Copy(conn, strings.NewReader(result)); err != nil {
				log.Fatalln("Error sending ECHO to the request", err.Error())
			}
		case "SET":
			p.NextType()
			key, err := p.NextBulkString()

			if err != nil {
				log.Fatalln("Error parsing key: ", err.Error())
			}

			p.NextType()
			value, err := p.NextBulkString()

			if err != nil {
				log.Fatalln("Error parsing value: ", err.Error())
			}

			db[key] = value

			ok := resp.NewSimpleString("OK").Encode()

			if _, err := io.Copy(conn, strings.NewReader(ok)); err != nil {
				log.Fatalln("Error sending OK to the request", err.Error())
			}
		case "GET":
			p.NextType()
			key, err := p.NextBulkString()
			if err != nil {
				log.Fatalln("Error parsing key: ", err.Error())
			}

			value, ok := db[key]

			if !ok {
				null := resp.NewNullBulkString().Encode()

				if _, err := io.Copy(conn, strings.NewReader(null)); err != nil {
					log.Fatalln("Error sending NULL to the request", err.Error())
				}
			} else {
				result := resp.NewBulkString(value).Encode()

				if _, err := io.Copy(conn, strings.NewReader(result)); err != nil {
					log.Fatalln("Error sending GET to the request", err.Error())
				}
			}

		default:
			fmt.Println("Unknown command", strconv.Quote(string(buf[:n])))
		}
	}
}
