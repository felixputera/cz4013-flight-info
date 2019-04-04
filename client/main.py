import argparse
import cmd
import datetime

from client.rpc.transport import UDPSocket
from client.rpc.protocol import BinaryProtocol
from client.client import Client
from client.rpc.exception import ApplicationException


parser = argparse.ArgumentParser(description="CZ4013 flight information system client")
parser.add_argument(
    "--outgoing_drop", "-o", type=float, help="Probability of sent UDP packet dropped"
)
parser.add_argument(
    "--incoming_drop",
    "-i",
    type=float,
    help="Probability of to be received UDP packet dropped",
)
parser.add_argument(
    "--timeout",
    "-t",
    type=int,
    help="Timeout duration for waiting response from server",
)
parser.add_argument(
    "--retry", "-r", type=int, help="Number of timeout retries before aborting"
)


class FlightShell(cmd.Cmd):
    intro = "Welcome to the flight info shell. Type help or ? to list commands.\n"
    prompt = "(flight) "

    def __init__(self, client):
        super().__init__()
        self.client = client

    def do_get(self, arg):
        "get flight by id: ID"
        try:
            flight = self.client.get_flight(*parse(arg))
            print("flight id:", flight.id)
            print("from:", flight.from_)
            print("to:", flight.to)
            print("time:", flight.time)
            print("num available seats:", flight.available_seats)
            print("ticket fare:", flight.fare)
        except ApplicationException as e:
            print(str(e))

    def do_reserve(self, arg):
        "reserve flight by id and seats: ID SEATS"
        try:
            self.client.reserve(*parse(arg))
            print("ok")
        except ApplicationException as e:
            print(str(e))

    def do_monitor_seats(self, arg):
        "monitor available seats: ID DURATION_IN_MS"
        flightid, duration_ms = parse(arg)
        duration_ms = int(duration_ms)

        self.client.send_monitor_seats(flightid, duration_ms)

        wait_until = datetime.datetime.now() + datetime.timedelta(
            milliseconds=duration_ms
        )
        while datetime.datetime.now() < wait_until:
            try:
                seats = self.client.recv_monitor_seats()
                print("available seats:", seats)
            except ApplicationException as e:
                print(str(e))

    def do_new(self, arg):
        "create new flight entry: ID FROM TO TIME AVAILABLE-SEATS FARE"
        try:
            self.client.new_flight(*parse(arg))
            print("ok")
        except ApplicationException as e:
            print(str(e))

    def do_find_flights(self, arg):
        "find flights: FROM TO"
        try:
            flight_ids = self.client.find_flights(*parse(arg))
            for flight_id in flight_ids:
                print(flight_id)
        except ApplicationException as e:
            print(str(e))

    def do_find_destinations(self, arg):
        "find destinations: FROM"
        try:
            destinations = self.client.find_destinations(*parse(arg))
            for destination in destinations:
                print(destination)
        except ApplicationException as e:
            print(str(e))


def parse(arg):
    "Convert string to an argument tuple"
    return tuple(arg.split())


if __name__ == "__main__":
    args = parser.parse_args()

    transport = UDPSocket()
    if args.timeout is not None:
        transport.set_timeout(args.timeout)
    if args.retry is not None:
        transport.set_num_retries(args.retry)
    if args.incoming_drop is not None:
        transport.set_incoming_drop(args.incoming_drop)
    if args.outgoing_drop is not None:
        transport.set_outgoing_drop(args.outgoing_drop)
    protocol = BinaryProtocol(transport)
    client = Client(protocol)

    transport.open()

    shell = FlightShell(client)
    shell.cmdloop()
