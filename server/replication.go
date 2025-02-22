package server

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/a7medev/goredis/resp"
)

type Replica struct {
	Conn
	Offset int
	mu     sync.Mutex
}

func (r *Replica) SetOffset(offset int) {
	r.mu.Lock()
	r.Offset = offset
	r.mu.Unlock()
}

type Replication struct {
	Replicas map[string]*Replica
}

func (r *Replication) AddReplica(conn Conn) {
	r.Replicas[conn.Addr()] = &Replica{Conn: conn}
}

// startReplication connects to the master server and starts the replication process.
func (s *Server) startReplication() {
	s.config.Mu.Lock()

	conn, err := connectToMaster(s.config.Replication.MasterHost, s.config.Replication.MasterPort)

	if err != nil {
		fmt.Println("Failed to connect to master", err)
		return
	}

	defer conn.Close()

	fmt.Println("Connected to master")

	buf := conn.Reader()
	parser := resp.NewParser(buf)

	// PING master
	err = pingMaster(conn, parser)

	if err != nil {
		fmt.Println("Failed to ping master", err)
		return
	}

	fmt.Println("Master replied with PONG, starting replication")

	err = sendReplConf(conn, parser, int(s.config.Server.Port))

	if err != nil {
		fmt.Println("Failed to send REPLCONF to master", err)
		return
	}

	fmt.Println("Sent REPLCONF to master")

	s.syncWithMaster(conn, parser)

	s.config.Mu.Unlock()

	fmt.Println("Finished syncing with master")

	s.handleMasterCommands(conn)
}

func connectToMaster(host string, port uint64) (Conn, error) {
	addr := fmt.Sprintf("%v:%v", host, port)

	c, err := net.Dial("tcp", addr)

	if err != nil {
		return nil, err
	}

	return NewNetConn(c), nil
}

func pingMaster(conn Conn, parser *resp.Parser) error {
	ping := createCommand("PING")
	err := conn.Reply(ping)

	if err != nil {
		return err
	}

	result, err := parser.NextSimpleString()

	if err != nil {
		return err
	}

	if result != "PONG" {
		return fmt.Errorf("master replied with %v", result)
	}

	return nil
}

func sendReplConf(conn Conn, parser *resp.Parser, port int) error {
	cmd := createCommand("REPLCONF", "listening-port", strconv.Itoa(port))
	err := conn.Reply(cmd)

	if err != nil {
		return err
	}

	result, err := parser.NextSimpleString()
	if err != nil {
		return err
	}

	if result != "OK" {
		return fmt.Errorf("master didn't reply with OK but rather with %v", result)
	}

	fmt.Println("Master replied with OK to REPLCONF listening-port")

	cmd = createCommand("REPLCONF", "capa", "eof", "capa", "psync2")
	err = conn.Reply(cmd)

	if err != nil {
		return err
	}

	result, err = parser.NextSimpleString()

	if err != nil {
		return err
	}

	if result != "OK" {
		return fmt.Errorf("master didn't reply with OK but rather with %v", result)
	}

	fmt.Println("Master replied with OK to REPLCONF capa psync2")

	return nil
}

func (s *Server) syncWithMaster(conn Conn, parser *resp.Parser) error {
	replId := s.config.Replication.MasterReplID
	offset := strconv.Itoa(s.config.Replication.MasterReplOffset)
	cmd := createCommand("PSYNC", replId, offset)
	err := conn.Reply(cmd)

	if err != nil {
		return err
	}

	// TODO: Read RDB from master and load it into the database
	// Should do so after implementing RDB encoding/decoding.

	result, err := parser.NextSimpleString()

	if err != nil {
		return err
	}

	fmt.Println("Master replied with", result)

	syncArgs := strings.Split(result, " ")

	switch syncArgs[0] {
	case "CONTINUE":
		fmt.Println("Master replied with CONTINUE, partial sync will follow")
	case "FULLRESYNC":
		fmt.Println("Master requested a full sync")

		replId := syncArgs[1]
		offset, err := strconv.Atoi(syncArgs[2])

		if err != nil {
			return err
		}

		s.config.Replication.MasterReplID = replId
		s.config.Replication.MasterReplOffset = offset

		// Read RDB file from server
		readRDB(conn)

		// TODO: update the server configuration with the RDB file.
		fmt.Println("Received RDB from master")

	default:
		return fmt.Errorf("invalid PSYNC result '%v' from master", result)
	}

	return nil
}

func readRDB(conn Conn) ([]byte, error) {
	buf := conn.Reader()

	t, err := buf.ReadByte()

	if err != nil {
		return nil, err
	}

	if t != '$' {
		return nil, fmt.Errorf("invalid RDB file format")
	}

	s, err := buf.ReadString('\r')

	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(s[:len(s)-1])

	if err != nil {
		return nil, err
	}

	// Discard \n
	buf.Discard(1)

	rdb := make([]byte, length)
	n, err := buf.Read(rdb)

	if err != nil {
		return nil, err
	}

	if n != length {
		return nil, fmt.Errorf("invalid RDB file format")
	}

	return rdb, nil
}

func (s *Server) handleMasterCommands(conn Conn) {
	fmt.Println("Listening for commands from master", conn.Addr())

	buf := conn.Reader()

	for {
		cmd, args, err := parseCommand(buf)

		if err == io.EOF {
			fmt.Println("Master closed connection", conn.Addr())
			return
		}

		ctx := s.newContext(conn, cmd, args, true)

		if err != nil {
			fmt.Println("Failed to parse master command", err)
			return
		}

		s.config.Mu.Lock()
		// TODO: refactor inefficient re-encoding on the command to get the length.
		s.config.Replication.MasterReplOffset += len(createCommand(cmd, args...).Encode())
		s.config.Mu.Unlock()

		handler, ok := s.commands[cmd]

		if ok {
			handler.Handler(ctx)
		} else {
			fmt.Printf("ERR unknown command '%v'\n", cmd)
		}
	}
}

func (s *Server) forwardToReplicas(cmd string, args ...string) {
	// TODO: Refactor to buffer commands and use ACKs to ensure all replicas received the command.
	// TODO: The client has already encoded the command while sending it to our server, so no need to
	// decode it and then encode it once again as we are doing here, we should refactor the parser to return
	// the original message sent by the client for logic like forwarding to be more efficient.
	msg := createCommand(cmd, args...)

	s.config.Mu.Lock()
	s.config.Replication.MasterReplOffset += len(msg.Encode())
	s.config.Mu.Unlock()

	for _, replica := range s.replication.Replicas {
		// NOTE: calling Reply with an Encodable each time is probably inefficient as it will encode the message each time.
		// as the message doesn't change.

		// Refactor to have ReplyString for example along with the default Reply which takes in an Encodable.
		replica.Reply(msg)
	}
}
