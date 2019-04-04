package rpc

import (
	"context"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type UdpServer struct {
	closed int32
	wg     sync.WaitGroup
	mu     sync.Mutex

	inputTransportFactory  TransportFactory
	outputTransportFactory TransportFactory
	inputProtocolFactory   ProtocolFactory
	outputProtocolFactory  ProtocolFactory
	processorFactory       ProcessorFactory
	serverTransport        ServerTransport
}

func NewUdpServer(processor Processor,
	serverTransport ServerTransport,
	transportFactory TransportFactory,
	protocolFactory ProtocolFactory) *UdpServer {
	return NewUdpServerFactory(NewProcessorFactory(processor),
		serverTransport,
		transportFactory,
		protocolFactory,
	)
}

func NewUdpServerFactory(processorFactory ProcessorFactory,
	serverTransport ServerTransport,
	transportFactory TransportFactory,
	protocolFactory ProtocolFactory) *UdpServer {

	return &UdpServer{
		processorFactory:       processorFactory,
		serverTransport:        serverTransport,
		inputTransportFactory:  transportFactory,
		outputTransportFactory: transportFactory,
		inputProtocolFactory:   protocolFactory,
		outputProtocolFactory:  protocolFactory,
	}
}

func (p *UdpServer) ProcessorFactory() ProcessorFactory {
	return p.processorFactory
}

func (p *UdpServer) ServerTransport() ServerTransport {
	return p.serverTransport
}

func (p *UdpServer) InputTransportFactory() TransportFactory {
	return p.inputTransportFactory
}

func (p *UdpServer) OutputTransportFactory() TransportFactory {
	return p.outputTransportFactory
}

func (p *UdpServer) InputProtocolFactory() ProtocolFactory {
	return p.inputProtocolFactory
}

func (p *UdpServer) OutputProtocolFactory() ProtocolFactory {
	return p.outputProtocolFactory
}

func (p *UdpServer) Listen() error {
	return p.serverTransport.Listen()
}

func (p *UdpServer) innerAccept() (int32, error) {
	client, err := p.serverTransport.Accept()
	p.mu.Lock()
	defer p.mu.Unlock()
	closed := atomic.LoadInt32(&p.closed)
	if closed != 0 {
		return closed, nil
	}
	if err != nil {
		log.Println("heeere")
		return 0, err
	}
	if client != nil {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			if err := p.processRequests(client); err != nil {
				log.Println("error processing request:", err)
			}
		}()
	}
	return 0, nil
}

func (p *UdpServer) AcceptLoop() error {
	for {
		closed, err := p.innerAccept()
		if err != nil {
			log.Println("OOOPS")
			return err
		}
		if closed != 0 {
			log.Println("cloooosing")
			return nil
		}
	}
}

func (p *UdpServer) Serve() error {
	err := p.Listen()
	if err != nil {
		return err
	}
	p.AcceptLoop()
	return nil
}

func (p *UdpServer) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if atomic.LoadInt32(&p.closed) != 0 {
		return nil
	}
	atomic.StoreInt32(&p.closed, 1)
	p.serverTransport.Interrupt()
	p.wg.Wait()
	return nil
}

func (p *UdpServer) processRequests(client Transport) error {
	processor := p.processorFactory.GetProcessor(client)
	inputTransport, e := p.inputTransportFactory.GetTransport(client)
	if e != nil {
		return e
	}
	outputTransport, e := p.outputTransportFactory.GetTransport(client)
	if e != nil {
		return e
	}
	inputProtocol := p.inputProtocolFactory.GetProtocol(inputTransport)
	outputProtocol := p.outputProtocolFactory.GetProtocol(outputTransport)
	defer func() {
		if e := recover(); e != nil {
			log.Printf("panic in processor: %s: %s", e, debug.Stack())
		}
	}()

	for {
		if atomic.LoadInt32(&p.closed) != 0 {
			return nil
		}
		ok, err := processor.Process(context.Background(), inputProtocol, outputProtocol)
		if ok {
			break
		} else {
			return err
		}
	}
	return nil
}
