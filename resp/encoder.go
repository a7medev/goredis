package resp

import "fmt"

type Encodable interface {
	Encode() string
}

type NullBulkString struct{}

func NewNullBulkString() *NullBulkString {
	return &NullBulkString{}
}

func (n *NullBulkString) Encode() string {
	return "$-1\r\n"
}

type NullArray struct{}

func NewNullArray() *NullArray {
	return &NullArray{}
}

func (n *NullArray) Encode() string {
	return "*-1\r\n"
}

type SimpleString struct {
	Value string
}

func NewSimpleString(value string) *SimpleString {
	return &SimpleString{Value: value}
}

func (s *SimpleString) Encode() string {
	return "+" + s.Value + "\r\n"
}

type BulkString struct {
	Value string
}

func NewBulkString(value string) *BulkString {
	return &BulkString{Value: value}
}

func (s *BulkString) Encode() string {
	return fmt.Sprintf("$%d\r\n%v\r\n", len(s.Value), s.Value)
}

type Integer struct {
	Value int
}

func NewInteger(value int) *Integer {
	return &Integer{Value: value}
}

func (i *Integer) Encode() string {
	return fmt.Sprintf(":%d\r\n", i.Value)
}

type SimpleError struct {
	Value string
}

func NewSimpleError(value string) *SimpleError {
	return &SimpleError{Value: value}
}

func (e *SimpleError) Encode() string {
	return "-" + e.Value + "\r\n"
}

type Array struct {
	Values []Encodable
}

func NewArray(values ...Encodable) *Array {
	return &Array{Values: values}
}

func (a *Array) Encode() string {
	str := fmt.Sprintf("*%d\r\n", len(a.Values))
	for _, v := range a.Values {
		str += v.Encode()
	}
	return str
}
