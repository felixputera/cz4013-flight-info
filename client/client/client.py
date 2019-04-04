from client.rpc.types import Type, MessageType
from client.rpc.exception import ApplicationException


class Flight(object):
    def __init__(self):
        self.id = ""
        self.from_ = ""  # from is a keyword in python
        self.to = ""
        self.time = ""
        self.availableSeats = 0
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
                self.availableSeats = iprot.read_i32()
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
