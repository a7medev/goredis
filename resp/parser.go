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

// TODO: is NextType needed? We can just embed that into Next* methods instead.
func (p *Parser) NextType() (DataType, error) {
	t, err := p.data.ReadByte()

	if err != nil {
		return 0, err
	}

	switch t {
	case '+':
		return SimpleStringType, nil
	case '-':
		return SimpleErrorType, nil
	case ':':
		return IntegerType, nil
	case '$':
		return BulkStringType, nil
	case '*':
		return ArrayType, nil
	case '_':
		return NullType, nil
	case '#':
		return BooleanType, nil
	case ',':
		return DoubleType, nil
	case '(':
		return BigNumberType, nil
	case '!':
		return BulkErrorType, nil
	case '=':
		return VerbatimStringType, nil
	case '%':
		return MapType, nil
	case '~':
		return SetType, nil
	case '>':
		return PushType, nil
	default:
		return 0, errors.New("invalid redis data type")
	}
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

func (p *Parser) NextSimpleString() (string, error) {
	return p.readUntilCRLF()
}

func (p *Parser) NextInteger() (int, error) {
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

func (p *Parser) NextBulkString() (string, error) {
	length, err := p.NextInteger()

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
