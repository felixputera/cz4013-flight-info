from client.rpc.types import Type, MessageType
from client.rpc.exception import ApplicationException


class Flight(object):
    def __init__(self):
        self.id = ""
        self.from_ = ""  # from is a keyword in python
        self.to = ""
        self.time = ""
        self.available_seats = 0
        self.fare = 0.0

    def read(self, iprot):
        while True:
            _, ftype, fid = iprot.read_field_begin()
            if ftype == Type.STOP:
                break
            if fid == 1 and ftype == Type.STRING:
                self.id = iprot.read_string()
            elif fid == 2 and ftype == Type.STRING:
                self.from_ = iprot.read_string()
            elif fid == 3 and ftype == Type.STRING:
                self.to = iprot.read_string()
            elif fid == 4 and ftype == Type.STRING:
                self.time = iprot.read_string()
            elif fid == 5 and ftype == Type.I32:
                self.available_seats = iprot.read_i32()
            elif fid == 6 and ftype == Type.FLOAT:
                self.fare = iprot.read_float()
            iprot.read_field_end()


class GetFlightArgs(object):
    def __init__(self):
        self.flightid = None

    def write(self, oprot):
        if self.flightid is not None:
            oprot.write_field_begin("id", Type.STRING, 1)
            oprot.write_string(self.flightid)
            oprot.write_field_end()
        oprot.write_field_stop()


class GetFlightResult(object):
    def __init__(self):
        self.flight = None

    def read(self, iprot):
        while True:
            _, ftype, fid = iprot.read_field_begin()
            if ftype == Type.STOP:
                break
            if fid == 1 and ftype == Type.STRUCT:
                self.flight = Flight()
                self.flight.read(iprot)
            iprot.read_field_end()


class ReserveArgs(object):
    def __init__(self):
        self.flightid = None
        self.seats = None

    def write(self, oprot):
        if self.flightid is not None:
            oprot.write_field_begin("id", Type.STRING, 1)
            oprot.write_string(self.flightid)
            oprot.write_field_end()
        if self.seats is not None:
            oprot.write_field_begin("seats", Type.I32, 2)
            oprot.write_i32(self.seats)
            oprot.write_field_end()
        oprot.write_field_stop()


class MonitorSeatsArgs(object):
    def __init__(self):
        self.flightid = None
        self.duration_ms = None

    def write(self, oprot):
        if self.flightid is not None:
            oprot.write_field_begin("id", Type.STRING, 1)
            oprot.write_string(self.flightid)
            oprot.write_field_end()
        if self.duration_ms is not None:
            oprot.write_field_begin("durationMs", Type.I32, 2)
            oprot.write_i32(self.duration_ms)
            oprot.write_field_end()
        oprot.write_field_stop()


class MonitorSeatsResult(object):
    def __init__(self):
        self.seats = None

    def read(self, iprot):
        while True:
            _, ftype, fid = iprot.read_field_begin()
            if ftype == Type.STOP:
                break
            if fid == 1 and ftype == Type.I32:
                self.seats = iprot.read_i32()
            iprot.read_field_end()


class NewFlightArgs(object):
    def __init__(self):
        self.flightid = None
        self.from_ = None
        self.to = None
        self.time = None
        self.available_seats = None
        self.fare = None

    def write(self, oprot):
        if self.flightid is not None:
            oprot.write_field_begin("id", Type.STRING, 1)
            oprot.write_string(self.flightid)
            oprot.write_field_end()
        if self.from_ is not None:
            oprot.write_field_begin("from", Type.STRING, 2)
            oprot.write_string(self.from_)
            oprot.write_field_end()
        if self.to is not None:
            oprot.write_field_begin("to", Type.STRING, 3)
            oprot.write_string(self.to)
            oprot.write_field_end()
        if self.time is not None:
            oprot.write_field_begin("time", Type.STRING, 4)
            oprot.write_string(self.time)
            oprot.write_field_end()
        if self.available_seats is not None:
            oprot.write_field_begin("availableSeats", Type.I32, 5)
            oprot.write_i32(self.available_seats)
            oprot.write_field_end()
        if self.fare is not None:
            oprot.write_field_begin("fare", Type.FLOAT, 6)
            oprot.write_float(self.fare)
            oprot.write_field_end()
        oprot.write_field_stop()


class FindFlightsArgs(object):
    def __init__(self):
        self.from_ = None
        self.to = None

    def write(self, oprot):
        if self.from_ is not None:
            oprot.write_field_begin("from", Type.STRING, 1)
            oprot.write_string(self.from_)
            oprot.write_field_end()
        if self.to is not None:
            oprot.write_field_begin("to", Type.STRING, 2)
            oprot.write_string(self.to)
            oprot.write_field_end()
        oprot.write_field_stop()


class FindFlightsResult(object):
    def __init__(self):
        self.flight_ids = None

    def read(self, iprot):
        while True:
            _, ftype, fid = iprot.read_field_begin()
            if ftype == Type.STOP:
                break
            if fid == 1 and ftype == Type.LIST:
                self.flight_ids = []
                _, size = iprot.read_list_begin()
                for _ in range(size):
                    self.flight_ids.append(iprot.read_string())
                iprot.read_list_end()
            iprot.read_field_end()

class FindDestinationsArgs(object):
    def __init__(self):
        self.from_ = None

    def write(self, oprot):
        if self.from_ is not None:
            oprot.write_field_begin("from", Type.STRING, 1)
            oprot.write_string(self.from_)
            oprot.write_field_end()
        oprot.write_field_stop()


class FindDestinationsResult(object):
    def __init__(self):
        self.destinations = None

    def read(self, iprot):
        while True:
            _, ftype, fid = iprot.read_field_begin()
            if ftype == Type.STOP:
                break
            if fid == 1 and ftype == Type.LIST:
                self.destinations = []
                _, size = iprot.read_list_begin()
                for _ in range(size):
                    self.destinations.append(iprot.read_string())
                iprot.read_list_end()
            iprot.read_field_end()


class Client(object):
    def __init__(self, iprot, oprot=None):
        self.iprot = self.oprot = iprot
        if oprot is not None:
            self.oprot = oprot
        self._seqid = 0

    @property
    def seqid(self):
        seqid = self._seqid
        self._seqid += 1
        return seqid

    def get_flight(self, flightid):
        self.send_get_flight(flightid)
        return self.recv_get_flight()

    def send_get_flight(self, flightid):
        self.oprot.write_message_begin("getFlight", MessageType.CALL, self.seqid)
        args = GetFlightArgs()
        args.flightid = flightid
        args.write(self.oprot)
        self.oprot.write_message_end()
        self.oprot.trans.flush()

    def recv_get_flight(self):
        _, mtype, _ = self.iprot.read_message_begin()
        if mtype == MessageType.EXCEPTION:
            e = ApplicationException()
            e.read(self.iprot)
            self.iprot.read_message_end()
            raise e

        result = GetFlightResult()
        result.read(self.iprot)
        self.iprot.read_message_end()

        return result.flight

    def reserve(self, flightid, seats):
        self.send_reserve(flightid, seats)
        self.recv_reserve()

    def send_reserve(self, flightid, seats):
        flightid = str(flightid)
        seats = int(seats)

        self.oprot.write_message_begin("reserve", MessageType.CALL, self.seqid)
        args = ReserveArgs()
        args.flightid = flightid
        args.seats = seats
        args.write(self.oprot)
        self.oprot.write_message_end()
        self.oprot.trans.flush()

    def recv_reserve(self):
        _, mtype, _ = self.iprot.read_message_begin()
        if mtype == MessageType.EXCEPTION:
            e = ApplicationException()
            e.read(self.iprot)
            self.iprot.read_message_end()
            raise e
        self.iprot.read_field_begin()  # for reading STOP
        self.iprot.read_message_end()

    def monitor_seats(self, flightid, duration_ms):
        flightid = str(flightid)
        duration_ms = int(duration_ms)
        self.send_reserve(flightid, duration_ms)
        self.recv_reserve()

    def send_monitor_seats(self, flightid, duration_ms):
        flightid = str(flightid)
        duration_ms = int(duration_ms)

        self.oprot.write_message_begin("monitorSeats", MessageType.CALL, self.seqid)
        args = MonitorSeatsArgs()
        args.flightid = flightid
        args.duration_ms = duration_ms
        args.write(self.oprot)
        self.oprot.write_message_end()
        self.oprot.trans.flush()

    def recv_monitor_seats(self):
        self.iprot.trans.listen = True
        self.iprot.trans.clear_bufs()

        _, mtype, _ = self.iprot.read_message_begin()
        if mtype == MessageType.EXCEPTION:
            e = ApplicationException()
            e.read(self.iprot)
            self.iprot.read_message_end()
            raise e

        result = MonitorSeatsResult()
        result.read(self.iprot)
        self.iprot.read_message_end()

        self.iprot.trans.listen = False

        return result.seats

    def new_flight(self, flightid, from_, to, time, available_seats, fare):
        self.send_new_flight(flightid, from_, to, time, available_seats, fare)
        self.recv_new_flight()

    def send_new_flight(self, flightid, from_, to, time, available_seats, fare):
        flightid = str(flightid)
        from_ = str(from_)
        to = str(to)
        time = str(time)
        available_seats = int(available_seats)
        fare = float(fare)

        self.oprot.write_message_begin("newFlight", MessageType.CALL, self.seqid)
        args = NewFlightArgs()
        args.flightid = flightid
        args.from_ = from_
        args.to = to
        args.time = time
        args.available_seats = available_seats
        args.fare = fare
        args.write(self.oprot)
        self.oprot.write_message_end()
        self.oprot.trans.flush()

    def recv_new_flight(self):
        _, mtype, _ = self.iprot.read_message_begin()
        if mtype == MessageType.EXCEPTION:
            e = ApplicationException()
            e.read(self.iprot)
            self.iprot.read_message_end()
            raise e
        self.iprot.read_field_begin()  # for reading STOP
        self.iprot.read_message_end()

    def find_flights(self, from_, to):
        self.send_find_flights(from_, to)
        return self.recv_find_flights()

    def send_find_flights(self, from_, to):
        from_ = str(from_)
        to = str(to)

        self.oprot.write_message_begin("findFlights", MessageType.CALL, self.seqid)
        args = FindFlightsArgs()
        args.from_ = from_
        args.to = to
        args.write(self.oprot)
        self.oprot.write_message_end()
        self.oprot.trans.flush()

    def recv_find_flights(self):
        _, mtype, _ = self.iprot.read_message_begin()
        if mtype == MessageType.EXCEPTION:
            e = ApplicationException()
            e.read(self.iprot)
            self.iprot.read_message_end()
            raise e

        result = FindFlightsResult()
        result.read(self.iprot)
        self.iprot.read_message_end()

        return result.flight_ids

    def find_destinations(self, from_):
        self.send_find_destinations(from_)
        return self.recv_find_destinations()

    def send_find_destinations(self, from_):
        from_ = str(from_)

        self.oprot.write_message_begin("findDestinations", MessageType.CALL, self.seqid)
        args = FindDestinationsArgs()
        args.from_ = from_
        args.write(self.oprot)
        self.oprot.write_message_end()
        self.oprot.trans.flush()

    def recv_find_destinations(self):
        _, mtype, _ = self.iprot.read_message_begin()
        if mtype == MessageType.EXCEPTION:
            e = ApplicationException()
            e.read(self.iprot)
            self.iprot.read_message_end()
            raise e

        result = FindDestinationsResult()
        result.read(self.iprot)
        self.iprot.read_message_end()

        return result.destinations
