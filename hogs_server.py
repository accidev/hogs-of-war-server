"""Lobby and relay server for Hogs of War network games.

Game clients connect over TCP, see the open rooms, create or join one, and send game messages to the
other players of their room through this server. The server knows nothing about the game: room and
player names, the room description, player data and messages are bytes it stores or passes on.
The wire format is in PROTOCOL.md.

    python hogs_server.py        settings come from the environment, see README.md
"""
import asyncio
import logging
import os
import signal
import struct
import time

PROTOCOL = 1
MAGIC = b'HOGS'
PORT = 47800
MAX_FRAME = 16384   # bytes of one frame after its length field
MAX_NAME = 63       # room name, player name
MAX_BLOB = 1024     # room description, player data (the game keeps a team there: 680 bytes)
MAX_PAYLOAD = 2048  # one game message
MAX_PLAYERS = 8     # per room; the game itself has four
HELLO_SECONDS = 10  # a new connection must send HELLO this soon
IDLE_SECONDS = 90   # a client that sends nothing this long is gone
RATE = 100          # frames per second from one client, sustained
BURST = 300
MAX_UNSENT = 256 * 1024  # a client that does not read is dropped at this many buffered bytes

# client -> server
HELLO, LIST, CREATE, JOIN, LEAVE, SEND, SET_ROOM, SET_PLAYER, PING = range(1, 10)
# server -> client
(WELCOME, ROOMS, JOINED, PLAYER_JOINED, PLAYER_LEFT, DATA, ROOM_CHANGED, PLAYER_DATA, OK, ERROR,
 PONG) = range(0x81, 0x8C)
# ERROR codes
(E_BAD, E_VERSION, E_NOT_FOUND, E_FULL, E_CLOSED, E_COMPAT, E_STATE, E_NOT_HOST, E_BUSY,
 E_RATE) = range(1, 11)

ROOM_CLOSED = 1  # room flag: nobody can join (the battle has started)

log = logging.getLogger('hogs')


def str8(data):
    return bytes([len(data)]) + data


def blob16(data):
    return struct.pack('<H', len(data)) + data


def frame(kind, payload=b''):
    return struct.pack('<HB', len(payload) + 1, kind) + payload


class Bad(Exception):
    """A frame that does not follow the protocol: the connection is closed."""


class Close(Exception):
    """The connection must end; the client has been told why."""


class Fields:
    """Reads the fields of one frame and refuses to read past its end."""

    def __init__(self, data):
        self.data = data
        self.at = 0

    def take(self, count):
        if self.at + count > len(self.data):
            raise Bad('frame too short')
        part = self.data[self.at:self.at + count]
        self.at += count
        return part

    def u8(self):
        return self.take(1)[0]

    def u16(self):
        return struct.unpack('<H', self.take(2))[0]

    def u32(self):
        return struct.unpack('<I', self.take(4))[0]

    def str8(self):
        data = self.take(self.u8())
        if len(data) > MAX_NAME:
            raise Bad('name too long')
        return data

    def blob16(self):
        size = self.u16()
        if size > MAX_BLOB:
            raise Bad('data too long')
        return self.take(size)

    def rest(self):
        return self.take(len(self.data) - self.at)

    def end(self):
        if self.at != len(self.data):
            raise Bad('frame too long')


class Client:
    def __init__(self, ident, writer, ip):
        self.id = ident  # also the player id in a room
        self.writer = writer
        self.ip = ip
        self.compat = None  # set by HELLO
        self.room = None
        self.name = b''
        self.data = b''
        self.tokens = float(BURST)
        self.stamp = time.monotonic()

    def allow(self):
        """One frame against the rate limit (a token bucket)."""
        now = time.monotonic()
        self.tokens = min(BURST, self.tokens + (now - self.stamp) * RATE)
        self.stamp = now
        if self.tokens < 1:
            return False
        self.tokens -= 1
        return True


class Room:
    def __init__(self, ident, host, name, limit, flags, desc):
        self.id = ident
        self.name = name
        self.limit = limit
        self.flags = flags
        self.desc = desc
        self.compat = host.compat
        self.players = [host]  # in the order they joined; the first one is the host


class Server:
    def __init__(self, name='Hogs of War lobby', max_clients=200, max_per_ip=16, max_rooms=100,
                 hello_seconds=HELLO_SECONDS, idle_seconds=IDLE_SECONDS):
        self.name = name.encode('ascii', 'replace')[:MAX_NAME]
        self.max_clients = max_clients
        self.max_per_ip = max_per_ip  # 0: no limit
        self.max_rooms = max_rooms
        self.hello_seconds = hello_seconds
        self.idle_seconds = idle_seconds
        self.clients = set()
        self.per_ip = {}
        self.rooms = {}
        self.next_client = 0x100  # DirectPlay keeps the small ids for itself
        self.next_room = 1
        self.handlers = {LIST: self.on_list, CREATE: self.on_create, JOIN: self.on_join, LEAVE: self.on_leave,
                         SEND: self.on_send, SET_ROOM: self.on_set_room, SET_PLAYER: self.on_set_player,
                         PING: self.on_ping}

    # connections

    async def serve(self, reader, writer):
        ip = (writer.get_extra_info('peername') or ('?',))[0]
        if len(self.clients) >= self.max_clients or (self.max_per_ip and self.per_ip.get(ip, 0) >= self.max_per_ip):
            log.info('%s refused: too many connections', ip)
            writer.write(frame(ERROR, bytes([0, E_BUSY]) + str8(b'server is full')))
            writer.close()
            return
        client = Client(self.next_client, writer, ip)
        self.next_client += 1
        self.clients.add(client)
        self.per_ip[ip] = self.per_ip.get(ip, 0) + 1
        try:
            await self.talk(client, reader)
        except Bad as why:
            log.info('%s #%d dropped: %s', ip, client.id, why)
            self.error(client, 0, E_BAD, str(why))
        except (Close, asyncio.TimeoutError, asyncio.IncompleteReadError, OSError):
            pass
        finally:
            self.leave(client)
            self.clients.discard(client)
            self.per_ip[ip] -= 1
            if not self.per_ip[ip]:
                del self.per_ip[ip]
            writer.close()

    async def talk(self, client, reader):
        wait = self.hello_seconds
        while True:
            (size,) = struct.unpack('<H', await asyncio.wait_for(reader.readexactly(2), wait))
            if not 1 <= size <= MAX_FRAME:
                raise Bad('bad frame size')
            body = await asyncio.wait_for(reader.readexactly(size), self.hello_seconds)
            if not client.allow():
                log.info('%s #%d dropped: too many frames', client.ip, client.id)
                self.error(client, body[0], E_RATE, 'too many frames')
                raise Close
            fields = Fields(body[1:])
            if client.compat is None:
                if body[0] != HELLO:
                    raise Bad('HELLO must come first')
                self.on_hello(client, fields)
            elif body[0] in self.handlers:
                self.handlers[body[0]](client, fields)
            else:
                raise Bad('unknown frame type %d' % body[0])
            wait = self.idle_seconds

    def send(self, client, data):
        transport = client.writer.transport
        if transport.is_closing():
            return
        if transport.get_write_buffer_size() > MAX_UNSENT:
            log.info('%s #%d dropped: does not read', client.ip, client.id)
            transport.abort()  # its reader fails next, and serve() cleans up
            return
        client.writer.write(data)

    def error(self, client, request, code, text):
        self.send(client, frame(ERROR, bytes([request, code]) + str8(text.encode('ascii')[:MAX_NAME])))

    def others(self, client):
        return [p for p in client.room.players if p is not client]

    def leave(self, client):
        room = client.room
        if not room:
            return
        room.players.remove(client)
        client.room = None
        client.name = client.data = b''
        if not room.players:
            del self.rooms[room.id]
            log.info('room %d closed', room.id)
            return
        log.info('room %d: #%d left (%d/%d)', room.id, client.id, len(room.players), room.limit)
        for player in room.players:
            self.send(player, frame(PLAYER_LEFT, struct.pack('<II', client.id, room.players[0].id)))

    def close(self):
        for client in list(self.clients):
            client.writer.transport.abort()

    # requests

    def on_hello(self, client, f):
        magic, protocol, compat = f.take(4), f.u16(), f.u32()
        f.end()
        if magic != MAGIC:
            raise Bad('not a Hogs of War client')
        if protocol != PROTOCOL:
            self.error(client, HELLO, E_VERSION, 'this server speaks protocol %d' % PROTOCOL)
            raise Close
        client.compat = compat
        self.send(client, frame(WELCOME, struct.pack('<HIHH', PROTOCOL, client.id, MAX_FRAME, self.idle_seconds)
                                + str8(self.name)))

    def on_list(self, client, f):
        f.end()
        chunk, count = b'', 0
        for room in self.rooms.values():
            item = (struct.pack('<IIBBB', room.id, room.compat, room.flags, len(room.players), room.limit)
                    + str8(room.name) + str8(room.players[0].name) + blob16(room.desc))
            if len(chunk) + len(item) > MAX_FRAME - 8:
                self.send(client, frame(ROOMS, struct.pack('<BH', 1, count) + chunk))
                chunk, count = b'', 0
            chunk += item
            count += 1
        self.send(client, frame(ROOMS, struct.pack('<BH', 0, count) + chunk))

    def joined(self, client):
        room = client.room
        players = b''.join(struct.pack('<I', p.id) + str8(p.name) + blob16(p.data) for p in room.players)
        self.send(client, frame(JOINED, struct.pack('<IIBB', room.id, room.players[0].id, room.flags, room.limit)
                                + str8(room.name) + blob16(room.desc) + bytes([len(room.players)]) + players))

    def on_create(self, client, f):
        limit, flags, name, desc, player = f.u8(), f.u8(), f.str8(), f.blob16(), f.str8()
        f.end()
        if not 1 <= limit <= MAX_PLAYERS:
            raise Bad('bad player limit')
        if client.room:
            return self.error(client, CREATE, E_STATE, 'already in a room')
        if len(self.rooms) >= self.max_rooms:
            return self.error(client, CREATE, E_BUSY, 'too many rooms')
        client.name = player
        client.room = self.rooms[self.next_room] = Room(self.next_room, client, name, limit, flags, desc)
        self.next_room += 1
        log.info('room %d opened by %s #%d: %r, up to %d players', client.room.id, client.ip, client.id, name, limit)
        self.joined(client)

    def on_join(self, client, f):
        ident, player = f.u32(), f.str8()
        f.end()
        room = self.rooms.get(ident)
        if client.room:
            return self.error(client, JOIN, E_STATE, 'already in a room')
        if not room:
            return self.error(client, JOIN, E_NOT_FOUND, 'no such room')
        if room.compat != client.compat:
            return self.error(client, JOIN, E_COMPAT, 'the room is for another version of the game')
        if room.flags & ROOM_CLOSED:
            return self.error(client, JOIN, E_CLOSED, 'the room is closed')
        if len(room.players) >= room.limit:
            return self.error(client, JOIN, E_FULL, 'the room is full')
        client.name = player
        client.room = room
        for other in room.players:
            self.send(other, frame(PLAYER_JOINED, struct.pack('<I', client.id) + str8(player) + blob16(b'')))
        room.players.append(client)
        log.info('room %d: %s #%d joined (%d/%d)', room.id, client.ip, client.id, len(room.players), room.limit)
        self.joined(client)

    def on_leave(self, client, f):
        f.end()
        if not client.room:
            return self.error(client, LEAVE, E_STATE, 'not in a room')
        self.leave(client)
        self.send(client, frame(OK, bytes([LEAVE])))

    def on_send(self, client, f):
        to, payload = f.u32(), f.rest()
        if not 1 <= len(payload) <= MAX_PAYLOAD:
            raise Bad('bad message size')
        if not client.room:
            return self.error(client, SEND, E_STATE, 'not in a room')
        data = frame(DATA, struct.pack('<II', client.id, to) + payload)
        for other in self.others(client):
            if not to or other.id == to:  # a message to a player who has left goes nowhere
                self.send(other, data)

    def on_set_room(self, client, f):
        flags, limit, desc = f.u8(), f.u8(), f.blob16()
        f.end()
        room = client.room
        if not room:
            return self.error(client, SET_ROOM, E_STATE, 'not in a room')
        if room.players[0] is not client:
            return self.error(client, SET_ROOM, E_NOT_HOST, 'only the host changes the room')
        if not len(room.players) <= limit <= MAX_PLAYERS:
            raise Bad('bad player limit')
        room.flags, room.limit, room.desc = flags, limit, desc
        for other in self.others(client):
            self.send(other, frame(ROOM_CHANGED, bytes([flags, limit]) + blob16(desc)))
        self.send(client, frame(OK, bytes([SET_ROOM])))

    def on_set_player(self, client, f):
        data = f.blob16()
        f.end()
        if not client.room:
            return self.error(client, SET_PLAYER, E_STATE, 'not in a room')
        client.data = data
        for other in self.others(client):
            self.send(other, frame(PLAYER_DATA, struct.pack('<I', client.id) + blob16(data)))
        self.send(client, frame(OK, bytes([SET_PLAYER])))

    def on_ping(self, client, f):
        echo = f.rest()
        if len(echo) > 16:
            raise Bad('ping too long')
        self.send(client, frame(PONG, echo))


async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    port = int(os.environ.get('HOGS_PORT', PORT))
    server = Server(name=os.environ.get('HOGS_NAME', 'Hogs of War lobby'),
                    max_clients=int(os.environ.get('HOGS_MAX_CLIENTS', 200)),
                    max_per_ip=int(os.environ.get('HOGS_MAX_PER_IP', 16)))
    listener = await asyncio.start_server(server.serve, None, port)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows: Ctrl+C raises KeyboardInterrupt instead
            pass
    log.info('listening on port %d, protocol %d', port, PROTOCOL)
    async with listener:
        await stop.wait()
        server.close()  # leaving the block waits for every connection to end
    log.info('stopped')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
