"""Asks a lobby server for its name and its rooms. Exit code 0 if it answers: the health check of the container.

    python check.py [host] [port]        default: 127.0.0.1 and HOGS_PORT or 47800
"""
import os
import socket
import struct
import sys

from hogs_server import ERROR, HELLO, LIST, MAGIC, PORT, PROTOCOL, ROOM_CLOSED, ROOMS, WELCOME, Fields, frame


def read(sock):
    def exactly(count):
        data = b''
        while len(data) < count:
            part = sock.recv(count - len(data))
            if not part:
                raise ConnectionError('the server closed the connection')
            data += part
        return data

    (size,) = struct.unpack('<H', exactly(2))
    body = exactly(size)
    if body[0] == ERROR:
        f = Fields(body[1:])
        f.take(2)
        raise ConnectionError('the server answered: ' + f.str8().decode('ascii', 'replace'))
    return body[0], Fields(body[1:])


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.environ.get('HOGS_PORT', PORT))
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(frame(HELLO, MAGIC + struct.pack('<HI', PROTOCOL, 0)) + frame(LIST))
        kind, f = read(sock)
        assert kind == WELCOME, kind
        protocol = f.u16()
        f.take(8)
        print('%s, protocol %d' % (f.str8().decode('ascii', 'replace'), protocol))
        more = 1
        while more:
            kind, f = read(sock)
            assert kind == ROOMS, kind
            more, count = f.u8(), f.u16()
            for _ in range(count):
                ident, compat, flags, players, limit = f.u32(), f.u32(), f.u8(), f.u8(), f.u8()
                name, owner = f.str8(), f.str8()
                f.blob16()
                print('  room %d: %r by %r, %d/%d players, game version %08X%s'
                      % (ident, name, owner, players, limit, compat, ', closed' if flags & ROOM_CLOSED else ''))


if __name__ == '__main__':
    try:
        main()
    except (OSError, AssertionError) as why:
        sys.exit('no answer: %s' % why)
