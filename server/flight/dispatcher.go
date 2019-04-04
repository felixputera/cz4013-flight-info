package flight

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/felixputera/cz4013-flight-info/server/rpc"
)

type FlightProcessor struct {
	methodMap map[string]rpc.ProcessorFunction
}

func NewFlightProcessor() *FlightProcessor {
	return &FlightProcessor{
		methodMap: map[string]rpc.ProcessorFunction{
			"getFlight":    &getFlightProcessor{},
			"reserve":      &reserveProcessor{},
			"monitorSeats": &monitorSeatsProcessor{},
		},
	}
}

func (p *FlightProcessor) Process(ctx context.Context, iprot, oprot rpc.Protocol) (bool, error) {
	name, _, seqID, err := iprot.ReadMessageBegin()
	if err != nil {
		return false, err
	}
	log.Println("received method", name)
	if proc, ok := p.methodMap[name]; ok {
		return proc.Process(ctx, seqID, iprot, oprot)
	}
	return false, errors.New("method not found")
}

type getFlightProcessor struct{}

type getFlightArgs struct {
	id string
}

func (a *getFlightArgs) read(iprot rpc.Protocol) (err error) {
	for {
		_, fieldType, fieldID, err := iprot.ReadFieldBegin()
		if err != nil {
			return rpc.PrependError(fmt.Sprintf("%T field %d read error: ", a, fieldID), err)
		}
		if fieldType == rpc.Stop {
			break
		}
		switch fieldID {
		case 1:
			if fieldType == rpc.String {
				a.id, err = iprot.ReadString()
				if err != nil {
					return rpc.PrependError("failed reading field 1 content", err)
				}
			} else {
				return errors.New("field 1 is not string type")
			}
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	return nil
}

type getFlightResult struct {
	flight *Flight
}

func (f *Flight) write(oprot rpc.Protocol) (err error) {
	if err = oprot.WriteFieldBegin("id", rpc.String, 1); err != nil {
		return
	}
	if err = oprot.WriteString(f.ID); err != nil {
		return
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return
	}
	if err = oprot.WriteFieldBegin("from", rpc.String, 2); err != nil {
		return
	}
	if err = oprot.WriteString(f.From); err != nil {
		return
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return
	}
	if err = oprot.WriteFieldBegin("to", rpc.String, 3); err != nil {
		return
	}
	if err = oprot.WriteString(f.To); err != nil {
		return
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return
	}
	if err = oprot.WriteFieldBegin("time", rpc.String, 4); err != nil {
		return
	}
	if err = oprot.WriteString(f.Time); err != nil {
		return
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return
	}
	if err = oprot.WriteFieldBegin("availableSeats", rpc.I32, 5); err != nil {
		return
	}
	if err = oprot.WriteI32(f.AvailabeSeats); err != nil {
		return
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return
	}
	if err = oprot.WriteFieldBegin("fare", rpc.Float, 6); err != nil {
		return
	}
	if err = oprot.WriteFloat(f.Fare); err != nil {
		return
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return
	}
	if err = oprot.WriteFieldStop(); err != nil {
		return
	}
	return nil
}

func (r *getFlightResult) write(oprot rpc.Protocol) (err error) {
	if err = oprot.WriteFieldBegin("flight", rpc.Struct, 1); err != nil {
		return
	}
	if err = r.flight.write(oprot); err != nil {
		return
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return
	}
	if err = oprot.WriteFieldStop(); err != nil {
		return
	}
	return nil
}

func (p *getFlightProcessor) Process(ctx context.Context, seqID int32, iprot, oprot rpc.Protocol) (bool, error) {
	// Read field arguments
	args := &getFlightArgs{}
	if err := args.read(iprot); err != nil {
		return false, err
	}
	if err := iprot.ReadMessageEnd(); err != nil {
		return false, err
	}

	// process
	flight, err := GetFlight(args.id)

	if err != nil {
		// handle exception in method, e.g. flight not found
		oprot.WriteMessageBegin("getFlight", rpc.Exception, seqID)
		appErr := rpc.NewApplicationException(rpc.InternalErrorID, "internal server error processing getFlight: "+err.Error())
		appErr.Write(oprot)
		oprot.WriteMessageEnd()
		oprot.Flush()
		return true, err
	}

	// success, write output
	res := &getFlightResult{flight: flight}
	if err = oprot.WriteMessageBegin("getFlight", rpc.Reply, seqID); err != nil {
		return false, err
	}
	if err = res.write(oprot); err != nil {
		return false, err
	}
	if err = oprot.WriteMessageEnd(); err != nil {
		return false, err
	}
	oprot.Flush()

	return true, nil
}

type reserveProcessor struct{}

type reserveArgs struct {
	id    string
	seats int32
}

func (a *reserveArgs) read(iprot rpc.Protocol) error {
	for {
		_, fieldType, fieldID, err := iprot.ReadFieldBegin()
		if err != nil {
			return rpc.PrependError(fmt.Sprintf("%T field %d read error: ", a, fieldID), err)
		}
		if fieldType == rpc.Stop {
			break
		}
		switch fieldID {
		case 1:
			if fieldType == rpc.String {
				a.id, err = iprot.ReadString()
				if err != nil {
					return rpc.PrependError("failed reading field 1 content", err)
				}
			} else {
				return errors.New("field 1 is not string type")
			}
		case 2:
			if fieldType == rpc.I32 {
				a.seats, err = iprot.ReadI32()
				if err != nil {
					return rpc.PrependError("failed reading field 2 content", err)
				}
			} else {
				return errors.New("field 2 is not int32 type")
			}
		}

		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	return nil
}

type reserveResult struct{}

func (r *reserveResult) write(oprot rpc.Protocol) error {
	return oprot.WriteFieldStop()
}

func (p *reserveProcessor) Process(ctx context.Context, seqID int32, iprot, oprot rpc.Protocol) (bool, error) {
	args := &reserveArgs{}
	if err := args.read(iprot); err != nil {
		return false, err
	}
	if err := iprot.ReadMessageEnd(); err != nil {
		return false, err
	}

	err := MakeReservation(args.id, args.seats)
	if err != nil {
		oprot.WriteMessageBegin("reserve", rpc.Exception, seqID)
		appErr := rpc.NewApplicationException(rpc.InternalErrorID, "internal server error processing reserve: "+err.Error())
		appErr.Write(oprot)
		err = oprot.WriteMessageEnd()
		if err != nil {
			return false, err
		}
		oprot.Flush()
		return true, err
	}

	res := &reserveResult{}
	if err := oprot.WriteMessageBegin("reserve", rpc.Reply, seqID); err != nil {
		return false, err
	}
	if err := res.write(oprot); err != nil {
		return false, err
	}
	if err = oprot.WriteMessageEnd(); err != nil {
		return false, err
	}
	oprot.Flush()

	return true, nil
}

type monitorSeatsProcessor struct{}

type monitorSeatsArgs struct {
	id         string
	durationMs int32
}

func (a *monitorSeatsArgs) read(iprot rpc.Protocol) error {
	for {
		_, fieldType, fieldID, err := iprot.ReadFieldBegin()
		if err != nil {
			return rpc.PrependError(fmt.Sprintf("%T field %d read error: ", a, fieldID), err)
		}
		if fieldType == rpc.Stop {
			break
		}
		switch fieldID {
		case 1:
			if fieldType == rpc.String {
				a.id, err = iprot.ReadString()
				if err != nil {
					return rpc.PrependError("failed reading field 1 content", err)
				}
			} else {
				return errors.New("field 1 is not string type")
			}
		case 2:
			if fieldType == rpc.I32 {
				a.durationMs, err = iprot.ReadI32()
				if err != nil {
					return rpc.PrependError("failed reading field 2 content", err)
				}
			} else {
				return errors.New("field 2 is not int32 type")
			}
		}

		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	return nil
}

type monitorSeatsResult struct {
	seats int32
}

func (r *monitorSeatsResult) write(oprot rpc.Protocol) (err error) {
	if err = oprot.WriteFieldBegin("seats", rpc.I32, 1); err != nil {
		return
	}
	if err = oprot.WriteI32(r.seats); err != nil {
		return
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return
	}
	if err = oprot.WriteFieldStop(); err != nil {
		return
	}
	return nil
}

func (p *monitorSeatsProcessor) Process(ctx context.Context, seqID int32, iprot, oprot rpc.Protocol) (bool, error) {
	args := &monitorSeatsArgs{}
	if err := args.read(iprot); err != nil {
		return false, err
	}
	if err := iprot.ReadMessageEnd(); err != nil {
		return false, err
	}

	resChan, errChan := MonitorAvailableSeats(args.id, args.durationMs)

	go func(seqID int32) {
		for {
			if resChan == nil && errChan == nil {
				oprot.WriteMessageBegin("reserve", rpc.Exception, seqID)
				appErr := rpc.NewApplicationException(rpc.InternalErrorID, "closing")
				appErr.Write(oprot)
				oprot.WriteMessageEnd()
				oprot.Flush()
				break
			}

			select {
			case seats, ok := <-resChan:
				if !ok {
					resChan = nil
					continue
				}
				res := &monitorSeatsResult{seats: seats}
				if err := oprot.WriteMessageBegin("reserve", rpc.Reply, seqID); err != nil {
					panic(err)
				}
				if err := res.write(oprot); err != nil {
					panic(err)
				}
				if err := oprot.WriteMessageEnd(); err != nil {
					panic(err)
				}
				oprot.Flush()
			case err, ok := <-errChan:
				if !ok {
					errChan = nil
					continue
				}
				oprot.WriteMessageBegin("reserve", rpc.Exception, seqID)
				appErr := rpc.NewApplicationException(rpc.InternalErrorID, "internal server error processing getFlight: "+err.Error())
				appErr.Write(oprot)
				oprot.WriteMessageEnd()
				oprot.Flush()
			}
		}
	}(seqID)

	return true, nil
}
