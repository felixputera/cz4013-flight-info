from client.rpc.types import Type

class ApplicationException(Exception):
    def __init__(self, message=""):
        super().__init__(message)

        self.message = message

    def __str__(self):
        return self.message

    def read(self, iprot):
        while True:
            _, ftype, fid = iprot.read_field_begin()
            if ftype == Type.STOP:
                break
            if fid == 1 and ftype == Type.STRING:
                self.message = iprot.read_string()
            iprot.read_field_end()
