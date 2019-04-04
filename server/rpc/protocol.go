package rpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// Interfaces

type ProtocolFactory interface {
	GetProtocol(trans Transport) Protocol
}

type Protocol interface {
	WriteMessageBegin(name string, typeID MessageType, seqID int32) error
	WriteMessageEnd() error
	WriteFieldBegin(name string, typeID Type, id int16) error
	WriteFieldEnd() error
	WriteFieldStop() error
	WriteListBegin(elemType Type, size int) error
	WriteListEnd() error
	WriteBool(value bool) error
	WriteByte(value byte) error
	WriteI16(value int16) error
	WriteI32(value int32) error
	WriteFloat(value float32) error
	WriteString(value string) error
	WriteBinary(value []byte) error

	ReadMessageBegin() (name string, typeID MessageType, seqID int32, err error)
	ReadMessageEnd() error
	ReadFieldBegin() (name string, typeID Type, id int16, err error)
	ReadFieldEnd() error
	ReadListBegin() (elemType Type, size int, err error)
	ReadListEnd() error
	ReadBool() (value bool, err error)
	ReadByte() (value byte, err error)
	ReadI16() (value int16, err error)
	ReadI32() (value int32, err error)
	ReadFloat() (value float32, err error)
	ReadString() (value string, err error)
	ReadBinary() (value []byte, err error)

	Flush() (err error)
	Transport() Transport
}

// Concrete binary protocol

type BinaryProtocol struct {
	// trans         TRichTransport
	trans *UDPSocket
	// reader        io.Reader
	// writer        io.Writer
	// strictRead  bool
	// strictWrite bool
	buffer [64]byte
}

type BinaryProtocolFactory struct{}

func NewBinaryProtocol(trans Transport) *BinaryProtocol {
	p := &BinaryProtocol{trans: trans.(*UDPSocket)}
	return p
}

func NewBinaryProtocolFactory() *BinaryProtocolFactory {
	return &BinaryProtocolFactory{}
}

func (p *BinaryProtocolFactory) GetProtocol(trans Transport) Protocol {
	return NewBinaryProtocol(trans)
}

// Write methods

func (p *BinaryProtocol) WriteBool(value bool) error {
	if value {
		return p.WriteByte(1)
	}
	return p.WriteByte(0)
}

func (p *BinaryProtocol) WriteByte(value byte) error {
	e := p.trans.WriteByte(value)
	return NewProtocolException(e)
}

func (p *BinaryProtocol) WriteI16(value int16) error {
	v := p.buffer[0:2]
	binary.BigEndian.PutUint16(v, uint16(value))
	_, e := p.trans.Write(v)
	return NewProtocolException(e)
}

func (p *BinaryProtocol) WriteI32(value int32) error {
	v := p.buffer[0:4]
	binary.BigEndian.PutUint32(v, uint32(value))
	_, e := p.trans.Write(v)
	return NewProtocolException(e)
}

func (p *BinaryProtocol) WriteFloat(value float32) error {
	return p.WriteI32(int32(math.Float32bits(value)))
}

func (p *BinaryProtocol) WriteString(value string) error {
	e := p.WriteI32(int32(len(value)))
	if e != nil {
		return NewProtocolException(e)
	}
	_, e = p.trans.WriteString(value)
	return NewProtocolException(e)
}

func (p *BinaryProtocol) WriteBinary(value []byte) error {
	e := p.WriteI32(int32(len(value)))
	if e != nil {
		return e
	}
	_, e = p.trans.Write(value)
	return NewProtocolException(e)
}

func (p *BinaryProtocol) WriteListBegin(elemType Type, size int) error {
	e := p.WriteByte(byte(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *BinaryProtocol) WriteListEnd() error {
	return nil
}

func (p *BinaryProtocol) WriteFieldBegin(name string, typeID Type, id int16) error {
	e := p.WriteByte(byte(typeID))
	if e != nil {
		return e
	}
	e = p.WriteI16(id)
	return e
}

func (p *BinaryProtocol) WriteFieldEnd() error {
	return nil
}

func (p *BinaryProtocol) WriteFieldStop() error {
	e := p.trans.WriteByte(byte(Stop))
	return e
}

func (p *BinaryProtocol) WriteMessageBegin(name string, typeID MessageType, seqID int32) error {
	e := p.WriteString(name)
	if e != nil {
		return e
	}
	e = p.WriteByte(byte(typeID))
	if e != nil {
		return e
	}
	e = p.WriteI32(seqID)
	return e
}

func (p *BinaryProtocol) WriteMessageEnd() error {
	return nil
}

// Read methods

func (p *BinaryProtocol) ReadMessageBegin() (name string, typeID MessageType, seqID int32, err error) {
	size, e := p.ReadI32()
	if e != nil {
		return "", typeID, 0, NewProtocolException(e)
	}
	if size < 0 {
		name, e = p.ReadString()
		if e != nil {
			return name, typeID, seqID, NewProtocolException(e)
		}
		seqID, e = p.ReadI32()
		if e != nil {
			return name, typeID, seqID, NewProtocolException(e)
		}
		return name, typeID, seqID, nil
	}
	name, e2 := p.readStringBody(size)
	if e2 != nil {
		return name, typeID, seqID, e2
	}
	b, e3 := p.ReadByte()
	if e3 != nil {
		return name, typeID, seqID, e3
	}
	typeID = MessageType(b)
	seqID, e4 := p.ReadI32()
	if e4 != nil {
		return name, typeID, seqID, e4
	}
	return name, typeID, seqID, nil
}

func (p *BinaryProtocol) ReadMessageEnd() error {
	return nil
}

func (p *BinaryProtocol) ReadFieldBegin() (name string, typeID Type, seqID int16, err error) {
	t, err := p.ReadByte()
	typeID = Type(t)
	if err != nil {
		return name, typeID, seqID, err
	}
	if Type(t) != Stop {
		seqID, err = p.ReadI16()
	}
	return name, typeID, seqID, err
}

func (p *BinaryProtocol) ReadFieldEnd() error {
	return nil
}

var invalidDataLength = NewProtocolExceptionWithType(InvalidDataID, errors.New("Invalid data length"))

func (p *BinaryProtocol) ReadListBegin() (elemType Type, size int, err error) {
	b, e := p.ReadByte()
	if e != nil {
		err = NewProtocolException(e)
		return
	}
	elemType = Type(b)
	size32, e := p.ReadI32()
	if e != nil {
		err = NewProtocolException(e)
		return
	}
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	size = int(size32)

	return
}

func (p *BinaryProtocol) ReadListEnd() error {
	return nil
}

func (p *BinaryProtocol) ReadBool() (bool, error) {
	b, e := p.ReadByte()
	v := true
	if b != 1 {
		v = false
	}
	return v, e
}

func (p *BinaryProtocol) ReadByte() (byte, error) {
	v, err := p.trans.ReadByte()
	return v, err
}

func (p *BinaryProtocol) ReadI16() (value int16, err error) {
	buf := p.buffer[0:2]
	err = p.readAll(buf)
	value = int16(binary.BigEndian.Uint16(buf))
	return value, err
}

func (p *BinaryProtocol) ReadI32() (value int32, err error) {
	buf := p.buffer[0:4]
	err = p.readAll(buf)
	value = int32(binary.BigEndian.Uint32(buf))
	return value, err
}

func (p *BinaryProtocol) ReadFloat() (value float32, err error) {
	buf := p.buffer[0:4]
	err = p.readAll(buf)
	value = math.Float32frombits(binary.BigEndian.Uint32(buf))
	return value, err
}

func (p *BinaryProtocol) ReadString() (value string, err error) {
	size, e := p.ReadI32()
	if e != nil {
		return "", e
	}
	if size < 0 {
		err = invalidDataLength
		return
	}

	return p.readStringBody(size)
}

func (p *BinaryProtocol) ReadBinary() ([]byte, error) {
	size, e := p.ReadI32()
	if e != nil {
		return nil, e
	}
	if size < 0 {
		return nil, invalidDataLength
	}

	isize := int(size)
	buf := make([]byte, isize)
	_, err := io.ReadFull(p.trans, buf)
	return buf, NewProtocolException(err)
}

func (p *BinaryProtocol) readAll(buf []byte) error {
	_, err := io.ReadFull(p.trans, buf)
	return NewProtocolException(err)
}

const readLimit = 32768

func (p *BinaryProtocol) readStringBody(size int32) (value string, err error) {
	if size < 0 {
		return "", nil
	}

	var (
		buf bytes.Buffer
		e   error
		b   []byte
	)

	switch {
	case int(size) <= len(p.buffer):
		b = p.buffer[:size] // avoids allocation for small reads
	case int(size) < readLimit:
		b = make([]byte, size)
	default:
		b = make([]byte, readLimit)
	}

	for size > 0 {
		_, e = io.ReadFull(p.trans, b)
		buf.Write(b)
		if e != nil {
			break
		}
		size -= readLimit
		if size < readLimit && size > 0 {
			b = b[:size]
		}
	}
	return buf.String(), NewProtocolException(e)
}

func (p *BinaryProtocol) Flush() (err error) {
	return NewProtocolException(p.trans.Flush())
}

func (p *BinaryProtocol) Transport() Transport {
	return p.trans
}
