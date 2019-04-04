package rpc

import (
	"bytes"
	"net"
)

type UDPSocket struct {
	conn     *net.UDPConn
	addr     net.Addr
	reqsaved []byte
	readbuf  *bytes.Buffer
	writebuf *bytes.Buffer

	saveResult bool
}

func NewUDPSocketFromConn(conn *net.UDPConn, addr net.Addr, buf []byte, saveResult bool) *UDPSocket {
	readbuf := bytes.NewBuffer(buf)
	return &UDPSocket{conn: conn, addr: addr, readbuf: readbuf, reqsaved: buf, saveResult: saveResult}
}

// Retrieve the underlying net.Conn
func (p *UDPSocket) Conn() net.Conn {
	return p.conn
}

// Returns true if the connection is open
func (p *UDPSocket) IsOpen() bool {
	if p.conn == nil {
		return false
	}
	return true
}

// Closes the socket.
func (p *UDPSocket) Close() error {
	// Close the socket
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			return err
		}
		p.conn = nil
	}
	return nil
}

//Returns the remote address of the socket.
func (p *UDPSocket) Addr() net.Addr {
	return p.addr
}

func (p *UDPSocket) Read(buf []byte) (int, error) {
	if !p.IsOpen() {
		return 0, NewTransportException(NotOpenID, "connection not open")
	}
	n, err := p.readbuf.Read(buf)
	return n, NewTransportExceptionFromError(err)
}

func (p *UDPSocket) Write(buf []byte) (int, error) {
	if p.writebuf == nil {
		p.writebuf = new(bytes.Buffer)
	}
	if !p.IsOpen() {
		return 0, NewTransportException(NotOpenID, "connection not open")
	}
	n, err := p.writebuf.Write(buf)
	return n, NewTransportExceptionFromError(err)
}

func (p *UDPSocket) Flush() error {
	buf := p.writebuf.Bytes()

	if p.saveResult {
		PutComputedResult(p.addr, p.reqsaved, buf)
	}

	_, err := p.conn.WriteTo(buf, p.addr)

	//reset writebuf
	p.writebuf = nil

	return err
}

func (p *UDPSocket) Address() net.Addr {
	return p.addr
}

func (p *UDPSocket) Interrupt() error {
	if !p.IsOpen() {
		return nil
	}
	return p.conn.Close()
}

func (p *UDPSocket) WriteString(s string) (int, error) {
	return p.writebuf.WriteString(s)
}

func (p *UDPSocket) WriteByte(c byte) error {
	return p.writebuf.WriteByte(c)
}

func (p *UDPSocket) ReadByte() (byte, error) {
	return p.readbuf.ReadByte()
}
