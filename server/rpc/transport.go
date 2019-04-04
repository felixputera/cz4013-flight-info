package rpc

import (
	"io"
	"net"
)

const (
	MaxBufferSize = 1024
)

// Transport interface to encapsulate the I/O layer
type Transport interface {
	io.ReadWriteCloser
	Flush() (err error)
	// Returns true if the transport is open
	IsOpen() bool
	// Returns current remote address
	Address() net.Addr
}

type ServerTransport interface {
	Listen() error
	Accept() (Transport, error)
	Close() error

	Interrupt() error
}

type TransportFactory interface {
	GetTransport(trans Transport) (Transport, error)
}

type transportFactory struct{}

func (p *transportFactory) GetTransport(trans Transport) (Transport, error) {
	return trans, nil
}

func NewTransportFactory() TransportFactory {
	return &transportFactory{}
}

// func Server(ctx context.Context, address string) (err error) {
// 	pc, err := net.ListenPacket("udp", address)
// 	if err != nil {
// 		return
// 	}
// 	// Close connection when exiting
// 	defer pc.Close()

// 	done := make(chan error, 1)
// 	buffer := make([]byte, MaxBufferSize)

// 	go func() {
// 		for {
// 			n, addr, err := pc.ReadFrom(buffer)
// 			if err != nil {
// 				done <- err
// 				return
// 			}

// 			// read buffer buffer[:n], process, and reply to addr
// 		}
// 	}()

// 	select {
// 	case <-ctx.Done():
// 		fmt.Println("Server closed")
// 		err = ctx.Err()
// 	case err = <-done:
// 		fmt.Println("Server error encountered")
// 	}

// 	return
// }
