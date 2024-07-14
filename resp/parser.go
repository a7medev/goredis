package resp

import (
	"bufio"
	"errors"
	"strconv"
)

var ErrNull = errors.New("null")

type Parser struct {
	data *bufio.Reader
}

func NewParser(data *bufio.Reader) *Parser {
	return &Parser{data: data}
}

func (p *Parser) readUntilCRLF() (string, error) {
	result, err := p.data.ReadString('\r')

	if err != nil {
		return "", err
	}

	// Skip the \n
	p.data.Discard(1)

	end := len(result) - 1
	return result[:end], nil
}

// readInteger parses the next integer from the buffer and returns it.
// It differs from NextInteger in that it doesn't read the type byte.
func (p *Parser) readInteger() (int, error) {
	result, err := p.readUntilCRLF()

	if err != nil {
		return 0, err
	}

	n, err := strconv.Atoi(result)

	if err != nil {
		return 0, err
	}

	return n, nil
}

func (p *Parser) NextInteger() (int, error) {
	t, err := p.data.ReadByte()

	if err != nil {
		return 0, err
	}

	if t != ':' {
		return 0, errors.New("invalid integer type")
	}

	return p.readInteger()
}

func (p *Parser) NextSimpleString() (string, error) {
	t, err := p.data.ReadByte()

	if err != nil {
		return "", err
	}

	if t != '+' {
		return "", errors.New("invalid simple string type")
	}

	return p.readUntilCRLF()
}

func (p *Parser) NextBulkString() (string, error) {
	t, err := p.data.ReadByte()

	if err != nil {
		return "", err
	}

	if t != '$' {
		return "", errors.New("invalid bulk string type")
	}

	length, err := p.readInteger()

	if err != nil {
		return "", err
	}

	if length == -1 {
		return "", ErrNull
	}

	result := make([]byte, length)
	n, err := p.data.Read(result)

	if err != nil {
		return "", err
	}

	if n != length {
		return "", errors.New("failed to read bulk string")
	}

	// Skip the \r\n
	p.data.Discard(2)

	return string(result), nil
}

func (p *Parser) NextArrayLength() (int, error) {
	t, err := p.data.ReadByte()

	if err != nil {
		return 0, err
	}

	if t != '*' {
		return 0, errors.New("invalid array type")
	}

	length, err := p.readInteger()

	if err != nil {
		return 0, err
	}

	if length == -1 {
		return 0, ErrNull
	}

	return length, nil
}
