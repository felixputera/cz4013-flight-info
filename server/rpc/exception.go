package rpc

import (
	"encoding/base64"
	"errors"
	"io"
)

// Transport exceptions

type BaseException interface {
	error
}

type timeoutable interface {
	Timeout() bool
}

type TransportException interface {
	BaseException
	TypeID() int
	Err() error
}

const (
	UnknownTransportExceptionID = 0
	NotOpenID                   = 1
	AlreadyOpenID               = 2
	TimedOutID                  = 3
	EndOfFileID                 = 4
)

type transportException struct {
	typeID int
	err    error
}

func (p *transportException) TypeID() int {
	return p.typeID
}

func (p *transportException) Error() string {
	return p.err.Error()
}

func (p *transportException) Err() error {
	return p.err
}

func NewTransportException(t int, e string) TransportException {
	return &transportException{typeID: t, err: errors.New(e)}
}

func NewTransportExceptionFromError(e error) TransportException {
	if e == nil {
		return nil
	}

	if t, ok := e.(TransportException); ok {
		return t
	}

	switch v := e.(type) {
	case TransportException:
		return v
	case timeoutable:
		if v.Timeout() {
			return &transportException{typeID: TimedOutID, err: e}
		}
	}

	if e == io.EOF {
		return &transportException{typeID: EndOfFileID, err: e}
	}

	return &transportException{typeID: UnknownTransportExceptionID, err: e}
}

// Application exceptions

const (
	UnknownApplicationExceptionID = 0
	UnknownMethodID               = 1
	InvalidMessageTypeExceptionID = 2
	WrongMethodNameID             = 3
	BadSequenceIDID               = 4
	MissingResultID               = 5
	InternalErrorID               = 6
	ProtocolErrorID               = 7
)

var defaultApplicationExceptionMessage = map[int32]string{
	UnknownApplicationExceptionID: "unknown application exception",
	UnknownMethodID:               "unknown method",
	InvalidMessageTypeExceptionID: "invalid message type",
	WrongMethodNameID:             "wrong method name",
	BadSequenceIDID:               "bad sequence ID",
	MissingResultID:               "missing result",
	InternalErrorID:               "unknown internal error",
	ProtocolErrorID:               "unknown protocol error",
}

type ApplicationException interface {
	BaseException
	TypeID() int32
	Read(iprot Protocol) error
	Write(oprot Protocol) error
}

type applicationException struct {
	message string
	typeID  int32
}

func (e applicationException) Error() string {
	if e.message != "" {
		return e.message
	}
	return defaultApplicationExceptionMessage[e.typeID]
}

func NewApplicationException(typeID int32, message string) ApplicationException {
	return &applicationException{message, typeID}
}

func (p *applicationException) TypeID() int32 {
	return p.typeID
}

func (p *applicationException) Read(iprot Protocol) error {
	message := ""
	typeID := int32(UnknownApplicationExceptionID)

	for {
		_, fieldType, fieldID, err := iprot.ReadFieldBegin()
		if err != nil {
			return err
		}
		if fieldType == Stop {
			break
		}
		switch fieldID {
		case 1:
			if fieldType == String {
				if message, err = iprot.ReadString(); err != nil {
					return err
				}
			}
		}
	}

	p.message = message
	p.typeID = typeID

	return nil
}

func (p *applicationException) Write(oprot Protocol) (err error) {
	if len(p.Error()) > 0 {
		if err = oprot.WriteFieldBegin("message", String, 1); err != nil {
			return
		}
		if err = oprot.WriteString(p.Error()); err != nil {
			return
		}
		if err = oprot.WriteFieldEnd(); err != nil {
			return
		}
	}
	err = oprot.WriteFieldStop()
	if err != nil {
		return
	}
	return
}

// Protocol exceptions

type ProtocolException interface {
	BaseException
	TypeID() int
}

const (
	UnknownProtocolExceptionID = 0
	InvalidDataID              = 1
	NegativeSizeID             = 2
	SizeLimitID                = 3
	NotImplementedID           = 4
	DepthLimitID               = 5
)

type protocolException struct {
	typeID  int
	message string
}

func (p *protocolException) TypeID() int {
	return p.typeID
}

func (p *protocolException) String() string {
	return p.message
}

func (p *protocolException) Error() string {
	return p.message
}

func NewProtocolException(err error) ProtocolException {
	if err == nil {
		return nil
	}
	if e, ok := err.(ProtocolException); ok {
		return e
	}
	if _, ok := err.(base64.CorruptInputError); ok {
		return &protocolException{InvalidDataID, err.Error()}
	}
	return &protocolException{UnknownProtocolExceptionID, err.Error()}
}

func NewProtocolExceptionWithType(errType int, err error) ProtocolException {
	if err == nil {
		return nil
	}
	return &protocolException{errType, err.Error()}
}

func PrependError(prepend string, err error) error {
	switch t := err.(type) {
	case TransportException:
		return NewTransportException(t.TypeID(), prepend+t.Error())
	case ProtocolException:
		return NewProtocolExceptionWithType(t.TypeID(), errors.New(prepend+err.Error()))
	case ApplicationException:
		return NewApplicationException(t.TypeID(), prepend+t.Error())
	}
	return errors.New(prepend + err.Error())
}
