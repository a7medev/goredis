package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/a7medev/goredis/config"
	"github.com/a7medev/goredis/resp"
	"github.com/a7medev/goredis/storage"
)

const BufferSize = 4096

type CommandHandler func(ctx *Context)

type Context struct {
	Config *config.Config
	Conn   net.Conn
	DB     *storage.Database
	Parser *resp.Parser
	Args   int
}

func NewContext(config *config.Config, conn net.Conn, db *storage.Database, parser *resp.Parser, args int) *Context {
	return &Context{
		Config: config,
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
	Config   *config.Config
	commands map[string]CommandHandler
}

func NewServer(cfg *config.Config) *Server {
	return &Server{
		Config:   cfg,
		commands: make(map[string]CommandHandler),
	}
}

// TODO: make the server exit gracefully.
func (s *Server) Start() {
	s.Config.Mu.RLock()

	addr := fmt.Sprintf(":%v", s.Config.Server.Port)
	ln, err := net.Listen("tcp", addr)

	if err != nil {
		log.Fatalln("Failed to bind to listen on address", addr)
	}

	fmt.Println("Listening on", addr)

	s.Listener = ln

	db := storage.NewDatabase()

	if s.Config.Replication.Role == config.RoleModeSlave {
		go s.startReplication()
	}

	s.Config.Mu.RUnlock()

	for {
		conn, err := s.Listener.Accept()

		if err != nil {
			log.Printf("Error accepting connection %v: %v\n", conn.RemoteAddr(), err.Error())
			continue
		}

		go s.handleConn(s.Config, conn, db)
	}
}

func (s *Server) AddCommand(cmd string, handler CommandHandler) {
	s.commands[cmd] = handler
}

func (s *Server) handleConn(config *config.Config, conn net.Conn, db *storage.Database) {
	defer conn.Close()

	fmt.Println("Connection from", conn.RemoteAddr())

	// TODO: Probably obtain buffer from a pool to reduce memory allocations.
	buf := make([]byte, BufferSize)
	for {
		// TODO: Clients could send multiple commands in one go.
		// We need to account for that by checking when we've read a full command if there's more data to read.
		// The current behavior expects a single command in a single read operation which is not always correct.
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

		ctx := NewContext(config, conn, db, p, cmdLen-1)
		if ok {
			handler(ctx)
		} else {
			fmt.Println("Unknown command", strconv.Quote(string(buf[:n])))
			msg := fmt.Sprintf("ERR unknown command '%v'", cmd)
			ctx.Reply(resp.NewSimpleError(msg))
		}
	}
}

// startReplication connects to the master server and starts the replication process.
// It sends the PING command to the master server to check if it's alive.
// It then sends the REPLCONF listening-port <port> and REPLCONF capa eof capa psync2 commands.
func (s *Server) startReplication() {
	s.Config.Mu.Lock()
	defer s.Config.Mu.Unlock()

	// Connect to master
	addr := fmt.Sprintf("%v:%v", s.Config.Replication.MasterHost, s.Config.Replication.MasterPort)
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		fmt.Println("Failed to connect to master", addr)
		return
	}

	defer conn.Close()

	fmt.Println("Connected to master", addr)

	// PING master
	ping := resp.NewArray(resp.NewBulkString("PING"))
	_, err = conn.Write([]byte(ping.Encode()))

	if err != nil {
		fmt.Println("Failed to PING master")
		return
	}

	buf := make([]byte, 256)
	n, err := conn.Read(buf)

	if err != nil {
		fmt.Println("Failed to read from master")
		return
	}

	p := resp.NewParser(buf, n)
	p.NextType()

	pong, err := p.NextSimpleString()

	if err != nil {
		fmt.Println("Failed to parse PONG from master")
		return
	}

	if pong != "PONG" {
		fmt.Println("Master didn't reply with PONG but rather with", pong)
		return
	}

	fmt.Println("Master replied with PONG, starting replication")

	// REPLCONF listening-port <port>
	// REPLCONF capa eof capa psync2

	replconf := resp.NewBulkString("REPLCONF")
	listiningPort := resp.NewBulkString("listening-port")
	port := resp.NewBulkString(strconv.Itoa(int(s.Config.Server.Port)))
	cmd := resp.NewArray(replconf, listiningPort, port)

	_, err = conn.Write([]byte(cmd.Encode()))

	if err != nil {
		fmt.Println("Failed to send REPLCONF listening-port to master")
		return
	}

	n, err = conn.Read(buf)

	if err != nil {
		fmt.Println("Failed to read from master")
		return
	}

	p = resp.NewParser(buf, n)
	p.NextType()
	ok, err := p.NextSimpleString()
	if err != nil {
		fmt.Println("Failed to parse result from master")
		return
	}

	if ok != "OK" {
		fmt.Println("Master didn't reply with OK but rather with", ok)
		return
	}

	fmt.Println("Master replied with OK to REPLCONF listening-port")

	capa := resp.NewBulkString("capa")
	eof := resp.NewBulkString("eof")
	psync2 := resp.NewBulkString("psync2")
	cmd = resp.NewArray(replconf, capa, eof, capa, psync2)

	_, err = conn.Write([]byte(cmd.Encode()))

	if err != nil {
		fmt.Println("Failed to send REPLCONF capa psync2 to master")
		return
	}

	n, err = conn.Read(buf)

	if err != nil {
		fmt.Println("Failed to read from master")
		return
	}

	p = resp.NewParser(buf, n)
	p.NextType()
	ok, err = p.NextSimpleString()

	if err != nil {
		fmt.Println("Failed to parse result from master")
		return
	}

	if ok != "OK" {
		fmt.Println("Master didn't reply with OK but rather with", ok)
		return
	}

	fmt.Println("Master replied with OK to REPLCONF capa psync2")

	// PSYNC <replicationId> <offset>

	psync := resp.NewBulkString("PSYNC")
	replId := resp.NewBulkString(s.Config.Replication.MasterReplID)
	offset := resp.NewBulkString(strconv.Itoa(s.Config.Replication.MasterReplOffset))
	cmd = resp.NewArray(psync, replId, offset)

	_, err = conn.Write([]byte(cmd.Encode()))

	if err != nil {
		fmt.Println("Failed to send PSYNC to master")
		return
	}

	// TODO: Read PSYNC reply from master
	// TODO: Read RDB from master and load it into the database
	// Should do so after implementing RDB encoding/decoding.

	n, err = conn.Read(buf)

	if err != nil {
		fmt.Println("Failed to read from master")
		return
	}

	p = resp.NewParser(buf, n)
	p.NextType()

	result, err := p.NextSimpleString()

	if err != nil {
		fmt.Println("Failed to parse PSYNC result from master")
		return
	}

	fmt.Println("Master replied with", result)

	syncArgs := strings.Split(result, " ")

	if syncArgs[0] == "FULLRESYNC" {
		fmt.Println("Master requested a full sync")

		replId := syncArgs[1]
		offset, err := strconv.Atoi(syncArgs[2])

		if err != nil {
			fmt.Println("Failed to parse offset from master")
			return
		}

		fmt.Println("Master replId:", replId, "offset:", offset)

		s.Config.Replication.MasterReplID = replId
		s.Config.Replication.MasterReplOffset = offset

		// Read RDB file from server
		// TODO: update the server configuration with the RDB file.
		// TODO: Adjust the buffer size as well to account for larger RDB files if needed.
		_, err = conn.Read(buf)

		if err != nil {
			fmt.Println("Failed to read RDB from master")
			return
		}

		fmt.Println("Received RDB from master")
	}
}
