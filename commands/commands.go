package commands

import (
	"fmt"
	"strconv"
	"strings"

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

	sections := []string{}

	for range ctx.Args {
		ctx.Parser.NextType()
		section, err := ctx.Parser.NextBulkString()

		if err != nil {
			fmt.Println("Error parsing section: ", err.Error())
			ctx.Reply(resp.NewSimpleError("ERR syntax error"))
			return
		}

		sections = append(sections, section)
	}

	for _, s := range ctx.Config.Sections {
		match := false

		if ctx.Args > 0 {
			for _, section := range sections {
				if s.Matches(section) {
					match = true
					break
				}
			}
		}

		if ctx.Args == 0 || match {
			b.WriteString(s.String())
			b.WriteByte('\n')
		}
	}

	info := resp.NewBulkString(b.String())
	ctx.Reply(info)
}
