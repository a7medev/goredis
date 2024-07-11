package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/a7medev/goredis/resp"
	"github.com/a7medev/goredis/storage"
)

const BufferSize = 4096

type CommandHandler func(conn *Connection, db *storage.Database, p *resp.Parser, args int)

type Connection struct {
	Conn net.Conn
}

func (c *Connection) Reply(reply resp.Encodable) error {
	s := strings.NewReader(reply.Encode())

	_, err := io.Copy(c.Conn, s)

	return err
}

type Server struct {
	Listener net.Listener
	Address  string
	commands map[string]CommandHandler
}

func NewServer(address string) *Server {
	return &Server{
		Address:  address,
		commands: make(map[string]CommandHandler),
	}
}

func (s *Server) Listen() {
	ln, err := net.Listen("tcp", s.Address)

	if err != nil {
		log.Fatalln("Failed to bind to listen on address", s.Address)
	}

	s.Listener = ln

	db := storage.NewDatabase()

	for {
		conn, err := s.Listener.Accept()

		if err != nil {
			log.Printf("Error accepting connection %v: %v\n", conn.RemoteAddr(), err.Error())
			continue
		}

		go s.handleConn(conn, db)
	}
}

func (s *Server) AddCommand(cmd string, handler CommandHandler) {
	s.commands[cmd] = handler
}

func (s *Server) handleConn(conn net.Conn, db *storage.Database) {
	defer conn.Close()

	fmt.Println("Connection from", conn.RemoteAddr())

	buf := make([]byte, BufferSize)
	for {
		n, err := conn.Read(buf)

		if err == io.EOF {
			fmt.Printf("Connection with %v closed.\n", conn.RemoteAddr())
			break
		}

		if err != nil {
			fmt.Printf("Error reading data from connection %v:%v\n", conn.RemoteAddr(), err.Error())
			return
		}

		p := resp.NewParser(buf, n)

		p.NextType()
		cmdLen, err := p.NextInteger()

		if err != nil {
			fmt.Println("Error parsing command:", err.Error())
		}

		p.NextType()
		cmd, err := p.NextBulkString()

		if err != nil {
			log.Fatalln("Error parsing command:", err.Error())
		}

		cmd = strings.ToUpper(cmd)

		handler, ok := s.commands[cmd]

		c := &Connection{Conn: conn}
		if ok {
			handler(c, db, p, cmdLen-1)
		} else {
			fmt.Println("Unknown command", strconv.Quote(string(buf[:n])))
			msg := fmt.Sprintf("ERR unknown command '%v'", cmd)
			c.Reply(resp.NewSimpleError(msg))
		}
	}
}
