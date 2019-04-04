import socket

import random

from client.config import Config


class TransportBase(object):
    def is_open(self):
        raise NotImplementedError

    def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def read(self, size):
        raise NotImplementedError

    def read_all(self, size):
        buf = b""
        have = 0
        while have < size:
            chunk = self.read(size - have)
            have += len(chunk)
            buf += chunk

            if len(chunk) == 0:
                raise EOFError()
        return buf

    def write(self, buf):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError


class UDPSocket(TransportBase):
    def __init__(self):
        self.host = Config.SERVER_HOST
        self.port = Config.SERVER_PORT
        self.handle = None
        self._socket_family = socket.AF_INET
        self._socket_type = socket.SOCK_DGRAM
        self._timeout = None
        self._num_retries = 0

        self.max_buf_size = Config.UDP_BUF_SIZE
        self._readbuf = None
        self._readbuf_offset = 0
        self._writebuf = None
        self._prev_writebuf = None

        self.listen = False

        self._outgoing_drop_rate = 0.0
        self._incoming_drop_rate = 0.0

    def set_handle(self, h):
        self.handle = h

    def is_open(self):
        return self.handle is not None

    def set_outgoing_drop(self, drop_rate):
        assert 0.0 <= drop_rate <= 1.0
        self._outgoing_drop_rate = drop_rate

    def set_incoming_drop(self, drop_rate):
        assert 0.0 <= drop_rate <= 1.0
        self._incoming_drop_rate = drop_rate

    def set_timeout(self, ms):
        if ms is None:
            self._timeout = None
        else:
            self._timeout = ms / 1000.0

        if self.handle is not None:
            self.handle.settimeout(self._timeout)

    def set_num_retries(self, num_retries):
        self._num_retries = num_retries

    def _do_open(self, family, socktype):
        return socket.socket(family, socktype)

    def open(self):
        if self.handle:
            pass  # TODO raise alreadyopenerror

        handle = self._do_open(self._socket_family, self._socket_type)
        handle.settimeout(self._timeout)

        self.handle = handle

    def _read_server(self):
        if self.listen:
            self.handle.settimeout(None)
        trial = 0
        while trial <= self._num_retries:
            try:
                buf, addr = self.handle.recvfrom(Config.UDP_BUF_SIZE)

                # randomly drop incoming packet
                if (
                    self._incoming_drop_rate > 0.0
                    and random.random() < self._incoming_drop_rate
                ):
                    print("\U000026D4 Incoming packet is dropped \U000026D4")
                    buf, addr = self.handle.recvfrom(Config.UDP_BUF_SIZE)
            except socket.timeout:
                if trial < self._num_retries:
                    print(f"receive timed out, retrying...")
                    self._writebuf = self._prev_writebuf
                    self.flush()
                trial += 1
            else:
                if addr == (self.host, self.port):
                    self._readbuf = buf
                    return
        if self.listen:
            self.handle.settimeout(self._timeout)
        raise Exception("aborting, cannot receive from server")

    def read(self, size):
        if self._readbuf is None:
            self._read_server()
        buf = self._readbuf[self._readbuf_offset : self._readbuf_offset + size]
        self._readbuf_offset += size
        return buf

    def write(self, buf):
        if self._writebuf is None:
            self._writebuf = bytearray()
        self._writebuf.extend(buf)

    def flush(self):
        if not self.handle:
            raise Exception("Socket not open")

        if (
            self._outgoing_drop_rate > 0.0
            and random.random() < self._outgoing_drop_rate
        ):
            print("\U000026D4 Outgoing packet is dropped \U000026D4")
        else:
            self.handle.sendto(bytes(self._writebuf), (self.host, self.port))

        self._prev_writebuf = self._writebuf
        self.clear_bufs()

    def clear_bufs(self):
        self._writebuf = None
        self._readbuf = None
        self._readbuf_offset = 0
