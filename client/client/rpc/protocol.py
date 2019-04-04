import struct
import socket

from client.rpc.types import Type


class ProtocolBase(object):
    def __init__(self, trans):
        self.trans = trans

    def write_message_begin(self, name, typeid, seqid):
        raise NotImplementedError

    def write_message_end(self):
        raise NotImplementedError

    def write_field_begin(self, name, typeid, fid):
        raise NotImplementedError

    def write_field_end(self):
        raise NotImplementedError

    def write_field_stop(self):
        raise NotImplementedError

    def write_list_begin(self, elemtype, size):
        raise NotImplementedError

    def write_list_end(self):
        raise NotImplementedError

    def write_bool(self, value):
        raise NotImplementedError

    def write_byte(self, value):
        raise NotImplementedError

    def write_i16(self, value):
        raise NotImplementedError

    def write_i32(self, value):
        raise NotImplementedError

    def write_float(self, value):
        raise NotImplementedError

    def write_string(self, value):
        raise NotImplementedError

    def write_binary(self, value):
        raise NotImplementedError

    def read_message_begin(self):
        raise NotImplementedError

    def read_message_end(self):
        raise NotImplementedError

    def read_field_begin(self):
        raise NotImplementedError

    def read_field_end(self):
        raise NotImplementedError

    def read_bool(self):
        raise NotImplementedError

    def read_byte(self):
        raise NotImplementedError

    def read_i16(self):
        raise NotImplementedError

    def raise_i32(self):
        raise NotImplementedError

    def read_float(self):
        raise NotImplementedError

    def read_string(self):
        raise NotImplementedError

    def read_binary(self):
        raise NotImplementedError
    
    def read_list_begin(self):
        raise NotImplementedError

    def read_list_end(self):
        raise NotImplementedError


class BinaryProtocol(ProtocolBase):
    def __init__(self, trans):
        super().__init__(trans)

    def write_message_begin(self, name, typeid, seqid):
        self.write_string(name)
        self.write_byte(typeid)
        self.write_i32(seqid)

    def write_message_end(self):
        pass

    def write_field_begin(self, name, typeid, fid):
        self.write_byte(typeid)
        self.write_i16(fid)

    def write_field_end(self):
        pass

    def write_field_stop(self):
        self.write_byte(Type.STOP)

    def write_list_begin(self, elemtype, size):
        self.write_byte(elemtype)
        self.write_i32(size)

    def write_list_end(self):
        pass

    def write_bool(self, value):
        self.write_byte(1 if value else 0)

    def write_byte(self, value):
        buf = struct.pack("!b", value)
        self.trans.write(buf)

    def write_i16(self, value):
        buf = struct.pack("!h", value)
        self.trans.write(buf)

    def write_i32(self, value):
        buf = struct.pack("!i", value)
        self.trans.write(buf)

    def write_float(self, value):
        buf = struct.pack("!f", value)
        self.trans.write(buf)

    def write_string(self, value):
        self.write_binary(bytes(value, "utf8"))

    def write_binary(self, value):
        self.write_i32(len(value))
        self.trans.write(value)

    def read_message_begin(self):
        size = self.read_i32()
        name = self.trans.read_all(size)
        typeid = self.read_byte()
        seqid = self.read_i32()

        return (name, typeid, seqid)

    def read_message_end(self):
        pass

    def read_field_begin(self):
        typeid = self.read_byte()
        if typeid == Type.STOP:
            return (None, typeid, 0)
        fid = self.read_i16()
        return (None, typeid, fid)

    def read_field_end(self):
        pass
    
    def read_list_begin(self):
        etype = self.read_byte()
        size = self.read_i32()
        return (etype, size)

    def read_list_end(self):
        pass

    def read_bool(self):
        byte = self.read_byte()
        return not not byte

    def read_byte(self):
        buf = self.trans.read_all(1)
        val, = struct.unpack("!b", buf)
        return val

    def read_i16(self):
        buf = self.trans.read_all(2)
        val, = struct.unpack("!h", buf)
        return val

    def read_i32(self):
        buf = self.trans.read_all(4)
        val, = struct.unpack("!i", buf)
        return val

    def read_float(self):
        buf = self.trans.read_all(4)
        val, = struct.unpack("!f", buf)
        return val

    def read_string(self):
        return self.read_binary().decode("utf8")

    def read_binary(self):
        size = self.read_i32()
        s = self.trans.read_all(size)
        return s
