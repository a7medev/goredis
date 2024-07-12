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

type CommandHandler func(ctx *Context)

type Context struct {
	Conn   net.Conn
	DB     *storage.Database
	Parser *resp.Parser
	Args   int
}

func NewContext(conn net.Conn, db *storage.Database, parser *resp.Parser, args int) *Context {
	return &Context{
		Conn:   conn,
		DB:     db,
		Parser: parser,
		Args:   args,
	}
}

func (c *Context) Reply(reply resp.Encodable) error {
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

	fmt.Println("Listening on", s.Address)

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

		ctx := NewContext(conn, db, p, cmdLen-1)
		if ok {
			handler(ctx)
		} else {
			fmt.Println("Unknown command", strconv.Quote(string(buf[:n])))
			msg := fmt.Sprintf("ERR unknown command '%v'", cmd)
			ctx.Reply(resp.NewSimpleError(msg))
		}
	}
}
