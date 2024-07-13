package commands

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/a7medev/goredis/rdb"
	"github.com/a7medev/goredis/resp"
	"github.com/a7medev/goredis/server"
	"github.com/a7medev/goredis/storage"
)

func Ping(ctx *server.Context) {
	pong := resp.NewSimpleString("PONG")
	ctx.Reply(pong)
}

func Echo(ctx *server.Context) {
	ctx.Parser.NextType()
	msg, err := ctx.Parser.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing command:", err.Error())
		return
	}

	result := resp.NewBulkString(msg)

	ctx.Reply(result)
}

func Set(ctx *server.Context) {
	ctx.Parser.NextType()
	key, err := ctx.Parser.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing key:", err.Error())
		ctx.Reply(resp.NewSimpleError("ERR syntax error"))
		return
	}

	ctx.Parser.NextType()
	value, err := ctx.Parser.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing value:", err.Error())
		ctx.Reply(resp.NewSimpleError("ERR syntax error"))
		return
	}

	expiry := storage.NeverExpires
	mode := storage.SetDefault
	get := false
	keepTTL := false

	// Parse extra arguments to SET
	for ctx.Args > 2 {
		ctx.Parser.NextType()
		arg, err := ctx.Parser.NextBulkString()

		if err != nil {
			fmt.Println("Error parsing argument:", err.Error())
			ctx.Reply(resp.NewSimpleError("ERR syntax error"))
			return
		}

		arg = strings.ToUpper(arg)

		switch arg {
		case "NX":
			if mode == storage.SetXX {
				fmt.Println("Error parsing argument: Can't use both NX and XX")
				ctx.Reply(resp.NewSimpleError("ERR syntax error"))
				return
			}

			mode = storage.SetNX
		case "XX":
			if mode == storage.SetNX {
				fmt.Println("Error parsing argument: Can't use both NX and XX")
				ctx.Reply(resp.NewSimpleError("ERR syntax error"))
				return
			}

			mode = storage.SetXX

		case "GET":
			get = true

		case "EX", "PX", "EXAT", "PXAT":
			ctx.Parser.NextType()
			timeStr, err := ctx.Parser.NextBulkString()

			ctx.Args--

			if err != nil {
				fmt.Println("Error parsing expiry:", err.Error())
				ctx.Reply(resp.NewSimpleError("ERR syntax error"))
				return
			}

			t, err := strconv.ParseInt(timeStr, 10, 64)

			if err != nil || t <= 0 {
				if err != nil {
					fmt.Println("Error parsing expiry:", err.Error())
				}
				ctx.Reply(resp.NewSimpleError("ERR invalid expire time in 'set' command"))
				return
			}

			expiry = storage.NewExpiry(t, arg)

		case "KEEPTTL":
			keepTTL = true

		default:
			fmt.Println("Error parsing argument: Unknown argument", arg)
			ctx.Reply(resp.NewSimpleError("ERR syntax error"))
			return
		}

		ctx.Args--
	}

	result, exists, isSet := ctx.DB.Set(key, value, expiry, mode, keepTTL, get)

	if get && exists {
		ctx.Reply(resp.NewBulkString(result))
	} else if get && !exists {
		ctx.Reply(resp.NewNullBulkString())
	} else if isSet {
		ctx.Reply(resp.NewSimpleString("OK"))
	} else {
		ctx.Reply(resp.NewNullBulkString())
	}
}

func Get(ctx *server.Context) {
	ctx.Parser.NextType()
	key, err := ctx.Parser.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing key: ", err.Error())
		return
	}

	value, ok := ctx.DB.Get(key)

	if !ok {
		null := resp.NewNullBulkString()

		ctx.Reply(null)
	} else {
		result := resp.NewBulkString(value)

		ctx.Reply(result)
	}
}

func Del(ctx *server.Context) {
	deleted := 0

	for i := 0; i < ctx.Args; i++ {
		ctx.Parser.NextType()
		key, err := ctx.Parser.NextBulkString()

		if err != nil {
			fmt.Println("Error parsing key: ", err.Error())
			ctx.Reply(resp.NewSimpleError("ERR syntax error"))
			return
		}

		if ok := ctx.DB.Delete(key); ok {
			deleted++
		}
	}

	ctx.Reply(resp.NewInteger(deleted))
}

func Info(ctx *server.Context) {
	b := strings.Builder{}

	outputServer := ctx.Args == 0
	outputReplication := ctx.Args == 0

	for range ctx.Args {
		ctx.Parser.NextType()
		section, err := ctx.Parser.NextBulkString()

		if err != nil {
			fmt.Println("Error parsing section: ", err.Error())
			ctx.Reply(resp.NewSimpleError("ERR syntax error"))
			return
		}

		switch strings.ToLower(section) {
		case "server":
			outputServer = true
		case "replication":
			outputReplication = true
		}
	}

	if outputServer {
		b.WriteString(ctx.Config.Server.String())
	}

	if outputReplication {
		b.WriteString(ctx.Config.Replication.String())
	}

	info := resp.NewBulkString(b.String())
	ctx.Reply(info)
}

func ReplConf(ctx *server.Context) {
	// TODO: handle REPLCONF arguments
	ctx.Reply(resp.NewSimpleString("OK"))
}

func PSync(ctx *server.Context) {
	replId := ctx.Config.Replication.MasterReplID
	replOffset := ctx.Config.Replication.MasterReplOffset

	result := fmt.Sprintf("FULLRESYNC %v %v", replId, replOffset)

	ctx.Reply(resp.NewSimpleString(result))

	// TODO: export a real RDB file once RDB is implemented into the server.
	// For now, we'll just send an empty RDB file.
	content, err := os.ReadFile("./empty.rdb")
	if err != nil {
		fmt.Println("Error reading RDB file:", err.Error())
		ctx.Reply(resp.NewSimpleError("ERR internal error"))
		return
	}

	ctx.Reply(rdb.NewRDB(content))
}
