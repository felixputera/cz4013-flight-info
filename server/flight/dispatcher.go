package flight

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/felixputera/cz4013-flight-info/server/rpc"
)

type Processor struct {
	methodMap map[string]rpc.ProcessorFunction
}

func NewProcessor() *Processor {
	return &Processor{
		methodMap: map[string]rpc.ProcessorFunction{
			"getFlight":        &getFlightProcessor{},
			"reserve":          &reserveProcessor{},
			"monitorSeats":     &monitorSeatsProcessor{},
			"findFlights":      &findFlightsProcessor{},
			"newFlight":        &newFlightProcessor{},
			"findDestinations": &findDestinationsProcessor{},
		},
	}
}

func (p *Processor) Process(ctx context.Context, iprot, oprot rpc.Protocol) (bool, error) {
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

	res := &voidResult{}
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

type newFlightProcessor struct{}

type newFlightArgs struct {
	id             string
	from           string
	to             string
	time           string
	availableSeats int32
	fare           float32
}

func (a *newFlightArgs) read(iprot rpc.Protocol) error {
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
			if fieldType == rpc.String {
				a.from, err = iprot.ReadString()
				if err != nil {
					return rpc.PrependError("failed reading field 2 content", err)
				}
			} else {
				return errors.New("field 2 is not string type")
			}
		case 3:
			if fieldType == rpc.String {
				a.to, err = iprot.ReadString()
				if err != nil {
					return rpc.PrependError("failed reading field 3 content", err)
				}
			} else {
				return errors.New("field 3 is not string type")
			}
		case 4:
			if fieldType == rpc.String {
				a.time, err = iprot.ReadString()
				if err != nil {
					return rpc.PrependError("failed reading field 4 content", err)
				}
			} else {
				return errors.New("field 4 is not string type")
			}
		case 5:
			if fieldType == rpc.I32 {
				a.availableSeats, err = iprot.ReadI32()
				if err != nil {
					return rpc.PrependError("failed reading field 5 content", err)
				}
			} else {
				return errors.New("field 5 is not i32 type")
			}
		case 6:
			if fieldType == rpc.Float {
				a.fare, err = iprot.ReadFloat()
				if err != nil {
					return rpc.PrependError("failed reading field 6 content", err)
				}
			} else {
				return errors.New("field 6 is not float type")
			}
		}

		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	return nil
}

type voidResult struct{}

func (r *voidResult) write(oprot rpc.Protocol) error {
	return oprot.WriteFieldStop()
}

func (p *newFlightProcessor) Process(ctx context.Context, seqID int32, iprot, oprot rpc.Protocol) (bool, error) {
	args := &newFlightArgs{}
	if err := args.read(iprot); err != nil {
		return false, err
	}
	if err := iprot.ReadMessageEnd(); err != nil {
		return false, err
	}

	_, err := NewFlight(args.id, args.from, args.to, args.time, args.availableSeats, args.fare)
	if err != nil {
		oprot.WriteMessageBegin("newFlight", rpc.Exception, seqID)
		appErr := rpc.NewApplicationException(rpc.InternalErrorID, "internal server error processing reserve: "+err.Error())
		appErr.Write(oprot)
		err = oprot.WriteMessageEnd()
		if err != nil {
			return false, err
		}
		oprot.Flush()
		return true, err
	}

	res := &voidResult{}
	if err := oprot.WriteMessageBegin("newFlight", rpc.Reply, seqID); err != nil {
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

type findDestinationsProcessor struct{}

type findDestinationsArgs struct {
	from string
}

func (a *findDestinationsArgs) read(iprot rpc.Protocol) error {
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
				a.from, err = iprot.ReadString()
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

type findDestinationsResult struct {
	destinations []string
}

func (r *findDestinationsResult) write(oprot rpc.Protocol) (err error) {
	if err = oprot.WriteFieldBegin("destinations", rpc.List, 1); err != nil {
		return
	}
	if err = oprot.WriteListBegin(rpc.String, len(r.destinations)); err != nil {
		return
	}
	for _, destination := range r.destinations {
		if err = oprot.WriteString(destination); err != nil {
			return
		}
	}
	if err = oprot.WriteListEnd(); err != nil {
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

func (p *findDestinationsProcessor) Process(ctx context.Context, seqID int32, iprot, oprot rpc.Protocol) (bool, error) {
	args := &findDestinationsArgs{}
	if err := args.read(iprot); err != nil {
		return false, err
	}
	if err := iprot.ReadMessageEnd(); err != nil {
		return false, err
	}

	destinations, err := FindDestinationsFrom(args.from)
	if err != nil {
		oprot.WriteMessageBegin("findDestinationss", rpc.Exception, seqID)
		appErr := rpc.NewApplicationException(rpc.InternalErrorID, "internal server error processing reserve: "+err.Error())
		appErr.Write(oprot)
		err = oprot.WriteMessageEnd()
		if err != nil {
			return false, err
		}
		oprot.Flush()
		return true, err
	}

	res := &findDestinationsResult{destinations: destinations}
	if err = oprot.WriteMessageBegin("findDestinationss", rpc.Reply, seqID); err != nil {
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

type findFlightsProcessor struct{}

type findFlightsArgs struct {
	from string
	to   string
}

func (a *findFlightsArgs) read(iprot rpc.Protocol) error {
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
				a.from, err = iprot.ReadString()
				if err != nil {
					return rpc.PrependError("failed reading field 1 content", err)
				}
			} else {
				return errors.New("field 1 is not string type")
			}
		case 2:
			if fieldType == rpc.String {
				a.to, err = iprot.ReadString()
				if err != nil {
					return rpc.PrependError("failed reading field 2 content", err)
				}
			} else {
				return errors.New("field 2 is not string type")
			}
		}

		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	return nil
}

type findFlightsResult struct {
	flightIDs []string
}

func (r *findFlightsResult) write(oprot rpc.Protocol) (err error) {
	if err = oprot.WriteFieldBegin("flightIDs", rpc.List, 1); err != nil {
		return
	}
	if err = oprot.WriteListBegin(rpc.String, len(r.flightIDs)); err != nil {
		return
	}
	for _, flightID := range r.flightIDs {
		if err = oprot.WriteString(flightID); err != nil {
			return
		}
	}
	if err = oprot.WriteListEnd(); err != nil {
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

func (p *findFlightsProcessor) Process(ctx context.Context, seqID int32, iprot, oprot rpc.Protocol) (bool, error) {
	args := &findFlightsArgs{}
	if err := args.read(iprot); err != nil {
		return false, err
	}
	if err := iprot.ReadMessageEnd(); err != nil {
		return false, err
	}

	flightIDs, err := FindFlightIDsFromTo(args.from, args.to)
	if err != nil {
		oprot.WriteMessageBegin("findFlights", rpc.Exception, seqID)
		appErr := rpc.NewApplicationException(rpc.InternalErrorID, "internal server error processing reserve: "+err.Error())
		appErr.Write(oprot)
		err = oprot.WriteMessageEnd()
		if err != nil {
			return false, err
		}
		oprot.Flush()
		return true, err
	}

	res := &findFlightsResult{flightIDs: flightIDs}
	if err = oprot.WriteMessageBegin("findFlights", rpc.Reply, seqID); err != nil {
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
