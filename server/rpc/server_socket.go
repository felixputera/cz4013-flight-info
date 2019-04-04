package rpc

import (
	"errors"
	"log"
	"net"
	"sync"
)

type ServerUDPSocket struct {
	conn *net.UDPConn
	addr *net.UDPAddr

	// Protects the interrupted value to make it thread safe.
	mu          sync.RWMutex
	interrupted bool

	filterDuplicate bool
}

func NewServerUDPSocket(listenAddr string, filterDuplicate bool) (*ServerUDPSocket, error) {
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	return &ServerUDPSocket{addr: addr, filterDuplicate: filterDuplicate}, nil
}

func (p *ServerUDPSocket) Listen() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.IsListening() {
		return nil
	}
	conn, err := net.ListenUDP(p.addr.Network(), p.addr)
	if err != nil {
		return err
	}
	p.conn = conn
	return nil
}

func (p *ServerUDPSocket) Accept() (Transport, error) {
	p.mu.RLock()
	interrupted := p.interrupted
	p.mu.RUnlock()

	if interrupted {
		return nil, errors.New("transport was interrupted")
	}

	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn == nil {
		return nil, NewTransportException(NotOpenID, "no underlying server socket")
	}

	for {
		buffer := make([]byte, MaxBufferSize)
		n, addr, err := p.conn.ReadFrom(buffer) // this is blocking
		if err != nil {
			return nil, err
		}
		buffer = buffer[:n]

		trans := NewUDPSocketFromConn(p.conn, addr, buffer, p.filterDuplicate)
		// directly return if result was previously computed
		if p.filterDuplicate {
			if result, ok := GetComputedResult(addr, buffer); ok {
				log.Println("returning previous computed result")
				trans.Write(result)
				trans.Flush()
				continue
			}
		}
		return trans, nil
	}
}

// Checks whether the socket is listening.
func (p *ServerUDPSocket) IsListening() bool {
	return p.conn != nil
}

func (p *ServerUDPSocket) Addr() net.Addr {
	if p.conn != nil {
		return p.conn.LocalAddr()
	}
	return p.addr
}

func (p *ServerUDPSocket) Close() error {
	var err error
	p.mu.Lock()
	if p.IsListening() {
		err = p.conn.Close()
		p.conn = nil
	}
	p.mu.Unlock()
	return err
}

func (p *ServerUDPSocket) Interrupt() error {
	p.mu.Lock()
	p.interrupted = true
	p.mu.Unlock()
	p.Close()

	return nil
}
