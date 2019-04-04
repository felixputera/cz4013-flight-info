package rpc

import (
	"context"
)

// A processor is a generic object which operates upon an input stream and
// writes to some output stream.
type Processor interface {
	Process(ctx context.Context, iprot, oprot Protocol) (bool, error)
}

type ProcessorFunction interface {
	Process(ctx context.Context, seqID int32, iprot, oprot Protocol) (bool, error)
}

// The default processor factory just returns a singleton
// instance.
type ProcessorFactory interface {
	GetProcessor(trans Transport) Processor
}

type processorFactory struct {
	processor Processor
}

func NewProcessorFactory(p Processor) ProcessorFactory {
	return &processorFactory{processor: p}
}

func (p *processorFactory) GetProcessor(trans Transport) Processor {
	return p.processor
}

type ProcessorFunctionFactory interface {
	GetProcessorFunction(trans Transport) ProcessorFunction
}

type processorFunctionFactory struct {
	processor ProcessorFunction
}

func NewProcessorFunctionFactory(p ProcessorFunction) ProcessorFunctionFactory {
	return &processorFunctionFactory{processor: p}
}

func (p *processorFunctionFactory) GetProcessorFunction(trans Transport) ProcessorFunction {
	return p.processor
}
