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
	if ctx.FromMaster {
		return
	}

	if len(ctx.Args) > 1 {
		ctx.Reply(resp.NewSimpleError("ERR wrong number of arguments for 'ping' command"))
		return
	}

	if len(ctx.Args) == 1 {
		msg := resp.NewBulkString(ctx.Args[0])
		ctx.Reply(msg)
	} else {
		pong := resp.NewSimpleString("PONG")
		ctx.Reply(pong)
	}
}

func Echo(ctx *server.Context) {
	if ctx.FromMaster {
		return
	}

	if len(ctx.Args) != 1 {
		ctx.Reply(resp.NewSimpleError("ERR wrong number of arguments for 'echo' command"))
		return
	}

	msg := ctx.Args[0]
	result := resp.NewBulkString(msg)

	ctx.Reply(result)
}

func Set(ctx *server.Context) {
	if len(ctx.Args) < 2 {
		if !ctx.FromMaster {
			ctx.Reply(resp.NewSimpleError("ERR wrong number of arguments for 'set' command"))
		}

		return
	}

	key := ctx.Args[0]
	value := ctx.Args[1]

	expiry := storage.NeverExpires
	mode := storage.SetDefault
	get := false
	keepTTL := false

	// Parse extra arguments to SET
	for i := 2; i < len(ctx.Args); i++ {
		arg := strings.ToUpper(ctx.Args[i])

		switch arg {
		case "NX":
			if mode == storage.SetXX {
				fmt.Println("Error parsing argument: Can't use both NX and XX")

				if !ctx.FromMaster {
					ctx.Reply(resp.NewSimpleError("ERR syntax error"))
				}

				return
			}

			mode = storage.SetNX
		case "XX":
			if mode == storage.SetNX {
				fmt.Println("Error parsing argument: Can't use both NX and XX")

				if !ctx.FromMaster {
					ctx.Reply(resp.NewSimpleError("ERR syntax error"))
				}

				return
			}

			mode = storage.SetXX

		case "GET":
			get = true

		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(ctx.Args) {
				fmt.Println("Error parsing argument: Missing expiry time")

				if !ctx.FromMaster {
					ctx.Reply(resp.NewSimpleError("ERR syntax error"))
				}

				return
			}

			t, err := strconv.ParseInt(ctx.Args[i+1], 10, 64)

			i++

			if err != nil || t <= 0 {
				if err != nil {
					fmt.Println("Error parsing expiry:", err.Error())
				}

				if !ctx.FromMaster {
					ctx.Reply(resp.NewSimpleError("ERR invalid expire time in 'set' command"))
				}
				return
			}

			expiry = storage.NewExpiry(t, arg)

		case "KEEPTTL":
			keepTTL = true

		default:
			fmt.Println("Error parsing argument: Unknown argument", arg)

			if !ctx.FromMaster {
				ctx.Reply(resp.NewSimpleError("ERR syntax error"))
			}

			return
		}
	}

	result, exists, isSet := ctx.DB.Set(key, value, expiry, mode, keepTTL, get)

	if ctx.FromMaster {
		return
	}

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
	if ctx.FromMaster {
		return
	}

	if len(ctx.Args) != 1 {
		ctx.Reply(resp.NewSimpleError("ERR wrong number of arguments for 'get' command"))
		return
	}

	key := ctx.Args[0]
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

	for _, key := range ctx.Args {
		if ok := ctx.DB.Delete(key); ok {
			deleted++
		}
	}

	if ctx.FromMaster {
		return
	}

	ctx.Reply(resp.NewInteger(deleted))
}

func Info(ctx *server.Context) {
	if ctx.FromMaster {
		return
	}

	b := strings.Builder{}

	outputServer := len(ctx.Args) == 0
	outputReplication := len(ctx.Args) == 0

	for _, section := range ctx.Args {
		switch strings.ToLower(section) {
		case "server":
			outputServer = true
		case "replication":
			outputReplication = true
		}
	}

	ctx.Config.Mu.RLock()

	if outputServer {
		b.WriteString(ctx.Config.Server.String())
	}

	if outputReplication {
		b.WriteString(ctx.Config.Replication.String())
	}

	ctx.Config.Mu.RUnlock()

	info := resp.NewBulkString(b.String())
	ctx.Reply(info)
}

func ReplConf(ctx *server.Context) {
	if ctx.FromMaster {
		return
	}

	// TODO: handle REPLCONF arguments
	ctx.Reply(resp.NewSimpleString("OK"))
}

func PSync(ctx *server.Context) {
	if ctx.FromMaster {
		return
	}

	ctx.Config.Mu.RLock()

	replId := ctx.Config.Replication.MasterReplID
	replOffset := ctx.Config.Replication.MasterReplOffset

	ctx.Config.Mu.RUnlock()

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

	ctx.Replcation.AddReplica(ctx.Conn)
}
