"""Tests of the lobby server: python -m unittest -v test_server

Each test starts a server on a free local port and talks to it as game clients would.
"""
import asyncio
import struct
import time
import types
import unittest
from unittest import mock

import hogs_server as hs
from hogs_server import Fields, blob16, frame, str8


class Peer:
    """A game client."""

    async def connect(self, port, compat=7, protocol=hs.PROTOCOL, magic=hs.MAGIC, hello=True, front=None):
        self.reader, self.writer = await asyncio.open_connection('127.0.0.1', port)
        if front:
            self.send(hs.FRONT, str8(front[0]) + str8(front[1]))
        if hello:
            self.send(hs.HELLO, magic + struct.pack('<HI', protocol, compat))
            kind, f = await self.recv()
            if kind == hs.WELCOME:
                f.u16()
                self.id = f.u32()
            return kind, f

    def send(self, kind, payload=b''):
        self.writer.write(frame(kind, payload))

    async def recv(self):
        """The next frame, or (None, None) when the server has closed the connection."""
        try:
            (size,) = struct.unpack('<H', await asyncio.wait_for(self.reader.readexactly(2), 3))
            body = await asyncio.wait_for(self.reader.readexactly(size), 3)
        except (asyncio.IncompleteReadError, ConnectionError):
            return None, None
        return body[0], Fields(body[1:])

    async def expect(self, kind):
        got, f = await self.recv()
        assert got == kind, 'expected frame %#x, got %s' % (kind, got and hex(got))
        return f

    async def error(self):
        f = await self.expect(hs.ERROR)
        return f.u8(), f.u8()

    async def create(self, name=b'room', limit=4, flags=0, desc=b'', player=b'host'):
        self.send(hs.CREATE, bytes([limit, flags]) + str8(name) + blob16(desc) + str8(player))
        return self.joined(await self.expect(hs.JOINED))

    async def join(self, room, player=b'guest'):
        self.send(hs.JOIN, struct.pack('<I', room) + str8(player))
        return self.joined(await self.expect(hs.JOINED))

    @staticmethod
    def joined(f):
        room = {'id': f.u32(), 'host': f.u32(), 'flags': f.u8(), 'limit': f.u8(), 'name': f.str8(), 'desc': f.blob16()}
        room['players'] = [(f.u32(), f.str8(), f.blob16()) for _ in range(f.u8())]
        f.end()
        return room

    async def rooms(self):
        self.send(hs.LIST)
        rooms, frames, more = [], 0, 1
        while more:
            f = await self.expect(hs.ROOMS)
            more, count = f.u8(), f.u16()
            frames += 1
            for _ in range(count):
                rooms.append({'id': f.u32(), 'compat': f.u32(), 'flags': f.u8(), 'players': f.u8(), 'limit': f.u8(),
                              'name': f.str8(), 'host': f.str8(), 'desc': f.blob16()})
            f.end()
        return rooms, frames


class ServerTest(unittest.IsolatedAsyncioTestCase):
    async def start(self, **settings):
        self.server = hs.Server(**{'free_loopback': False, **settings})  # every peer of a test is a local one
        self.listener = await asyncio.start_server(self.server.serve, '127.0.0.1', 0)
        self.port = self.listener.sockets[0].getsockname()[1]

    async def asyncSetUp(self):
        self.peers = []
        await self.start()

    async def asyncTearDown(self):
        for peer in self.peers:
            peer.writer.close()
        await self.server.close()
        self.listener.close()

    async def peer(self, **how):
        peer = Peer()
        self.peers.append(peer)
        peer.first = await peer.connect(self.port, **how)
        return peer

    async def settled(self):
        """Lets the server finish what the closed connections left to do."""
        for _ in range(50):
            await asyncio.sleep(0.01)

    async def test_hello(self):
        a = await self.peer()
        self.assertEqual(a.first[0], hs.WELCOME)
        self.assertGreaterEqual(a.id, 0x100)
        b = await self.peer(protocol=99)
        self.assertEqual((b.first[0], b.first[1].u8(), b.first[1].u8()), (hs.ERROR, hs.HELLO, hs.E_VERSION))
        self.assertEqual(await b.recv(), (None, None))
        c = await self.peer(magic=b'HTTP')
        self.assertEqual((c.first[0], c.first[1].u8(), c.first[1].u8()), (hs.ERROR, 0, hs.E_BAD))
        self.assertEqual(await c.recv(), (None, None))
        d = await self.peer(hello=False)
        d.send(hs.LIST)
        self.assertEqual(await d.error(), (0, hs.E_BAD))
        self.assertEqual(await d.recv(), (None, None))

    async def test_create_and_list(self):
        a, b = await self.peer(compat=7), await self.peer(compat=8)
        room = await a.create(b'first', limit=4, desc=b'\x01\x02', player=b'ann')
        self.assertEqual((room['host'], room['players']), (a.id, [(a.id, b'ann', b'')]))
        rooms, _ = await b.rooms()
        self.assertEqual(rooms, [{'id': room['id'], 'compat': 7, 'flags': 0, 'players': 1, 'limit': 4, 'name': b'first',
                                  'host': b'ann', 'desc': b'\x01\x02'}])
        a.send(hs.CREATE, bytes([4, 0]) + str8(b'second') + blob16(b'') + str8(b'ann'))
        self.assertEqual(await a.error(), (hs.CREATE, hs.E_STATE))

    async def test_join_and_relay(self):
        a, b, c, d = [await self.peer() for _ in range(4)]
        room = await a.create()
        joined = await b.join(room['id'], b'bob')
        self.assertEqual((joined['host'], joined['players']), (a.id, [(a.id, b'host', b''), (b.id, b'bob', b'')]))
        f = await a.expect(hs.PLAYER_JOINED)
        self.assertEqual((f.u32(), f.str8()), (b.id, b'bob'))
        await c.join(room['id'], b'cy')
        await a.expect(hs.PLAYER_JOINED)
        await b.expect(hs.PLAYER_JOINED)
        await d.create(b'other')

        a.send(hs.SEND, struct.pack('<I', 0) + b'to all')
        for peer in b, c:
            f = await peer.expect(hs.DATA)
            self.assertEqual((f.u32(), f.u32(), f.rest()), (a.id, 0, b'to all'))
        b.send(hs.SEND, struct.pack('<I', c.id) + b'to cy')
        f = await c.expect(hs.DATA)
        self.assertEqual((f.u32(), f.u32(), f.rest()), (b.id, c.id, b'to cy'))
        b.send(hs.SEND, struct.pack('<I', 12345) + b'to nobody')
        # nothing else reached anybody: the sender, the third player, the other room
        for peer in a, b, c, d:
            peer.send(hs.PING, b'x')
            self.assertEqual((await peer.expect(hs.PONG)).rest(), b'x')

    async def test_refused_joins(self):
        a, b, c, old = await self.peer(), await self.peer(), await self.peer(), await self.peer(compat=1)
        room = await a.create(limit=2)
        b.send(hs.JOIN, struct.pack('<I', 999) + str8(b'bob'))
        self.assertEqual(await b.error(), (hs.JOIN, hs.E_NOT_FOUND))
        old.send(hs.JOIN, struct.pack('<I', room['id']) + str8(b'old'))
        self.assertEqual(await old.error(), (hs.JOIN, hs.E_COMPAT))
        await b.join(room['id'])
        await a.expect(hs.PLAYER_JOINED)
        c.send(hs.JOIN, struct.pack('<I', room['id']) + str8(b'cy'))
        self.assertEqual(await c.error(), (hs.JOIN, hs.E_FULL))

        b.send(hs.SET_ROOM, bytes([hs.ROOM_CLOSED, 4]) + blob16(b''))
        self.assertEqual(await b.error(), (hs.SET_ROOM, hs.E_NOT_HOST))
        a.send(hs.SET_ROOM, bytes([hs.ROOM_CLOSED, 4]) + blob16(b'started'))
        self.assertEqual((await a.expect(hs.OK)).u8(), hs.SET_ROOM)
        f = await b.expect(hs.ROOM_CHANGED)
        self.assertEqual((f.u8(), f.u8(), f.blob16()), (hs.ROOM_CLOSED, 4, b'started'))
        c.send(hs.JOIN, struct.pack('<I', room['id']) + str8(b'cy'))
        self.assertEqual(await c.error(), (hs.JOIN, hs.E_CLOSED))
        rooms, _ = await c.rooms()
        self.assertEqual((rooms[0]['flags'], rooms[0]['desc']), (hs.ROOM_CLOSED, b'started'))

    async def test_leave_and_disconnect(self):
        a, b, c, watcher = [await self.peer() for _ in range(4)]
        room = await a.create()
        await b.join(room['id'])
        await c.join(room['id'])
        await a.expect(hs.PLAYER_JOINED)
        await a.expect(hs.PLAYER_JOINED)
        await b.expect(hs.PLAYER_JOINED)

        a.send(hs.LEAVE)  # the host leaves: the next oldest player is the host
        self.assertEqual((await a.expect(hs.OK)).u8(), hs.LEAVE)
        for peer in b, c:
            f = await peer.expect(hs.PLAYER_LEFT)
            self.assertEqual((f.u32(), f.u32()), (a.id, b.id))
        a.send(hs.SEND, struct.pack('<I', 0) + b'late')
        self.assertEqual(await a.error(), (hs.SEND, hs.E_STATE))

        b.writer.close()  # a lost connection is a leave
        f = await c.expect(hs.PLAYER_LEFT)
        self.assertEqual((f.u32(), f.u32()), (b.id, c.id))
        self.assertEqual((await watcher.rooms())[0][0]['players'], 1)
        c.writer.close()
        await self.settled()
        self.assertEqual((await watcher.rooms())[0], [])
        self.assertEqual(len(self.server.clients), 2)  # the one who left the room and the watcher

    async def test_player_data(self):
        a, b, c = await self.peer(), await self.peer(), await self.peer()
        room = await a.create()
        await b.join(room['id'])
        await a.expect(hs.PLAYER_JOINED)
        team = bytes(range(256)) * 2 + bytes(168)  # 680 bytes: a team of the game
        b.send(hs.SET_PLAYER, blob16(team))
        self.assertEqual((await b.expect(hs.OK)).u8(), hs.SET_PLAYER)
        f = await a.expect(hs.PLAYER_DATA)
        self.assertEqual((f.u32(), f.blob16()), (b.id, team))
        self.assertEqual((await c.join(room['id']))['players'][1], (b.id, b'guest', team))

    async def test_many_rooms(self):
        self.listener.close()
        await self.start(max_rooms=40, max_per_ip=0)
        for i in range(41):
            peer = await self.peer()
            peer.send(hs.CREATE, bytes([4, 0]) + str8(b'r%02d' % i + b'n' * 59) + blob16(b'd' * hs.MAX_BLOB) + str8(b'p' * 63))
            if i < 40:
                await peer.expect(hs.JOINED)
            else:
                self.assertEqual(await peer.error(), (hs.CREATE, hs.E_BUSY))
        rooms, frames = await peer.rooms()
        self.assertEqual([room['name'][:3] for room in rooms], [b'r%02d' % i for i in range(40)])
        self.assertGreater(frames, 1)

    async def test_limits(self):
        self.listener.close()
        await self.start(max_per_ip=3, hello_seconds=0.3)
        a, b, quiet = await self.peer(), await self.peer(), await self.peer(hello=False)
        refused = await self.peer(hello=False)
        self.assertEqual(await refused.error(), (0, hs.E_BUSY))
        self.assertEqual(await quiet.recv(), (None, None))  # no HELLO in time

        a.writer.write(struct.pack('<H', hs.MAX_FRAME + 1))
        self.assertEqual(await a.error(), (0, hs.E_BAD))
        self.assertEqual(await a.recv(), (None, None))

        # more frames than the rate allows are all answered, the last ones later
        started = time.monotonic()
        b.writer.write(frame(hs.PING) * (hs.BURST + 50))
        for _ in range(hs.BURST + 50):
            self.assertEqual((await b.recv())[0], hs.PONG)
        self.assertGreater(time.monotonic() - started, 40 / hs.RATE)
        b.writer.close()
        await self.settled()
        self.assertEqual(self.server.per_ip, {})

    async def test_bad_frames(self):
        for payload in (bytes([0, 0]) + str8(b'x') + blob16(b'') + str8(b'p'),        # no players allowed
                        bytes([4, 0]) + bytes([64]) + b'n' * 64 + blob16(b'') + str8(b'p'),  # name too long
                        bytes([4, 0]) + str8(b'x') + blob16(b'd' * (hs.MAX_BLOB + 1)) + str8(b'p'),   # description too long
                        bytes([4, 0]) + str8(b'x'),                                    # cut short
                        bytes([4, 0]) + str8(b'x') + blob16(b'') + str8(b'p') + b'!'):  # extra bytes
            peer = await self.peer()
            peer.send(hs.CREATE, payload)
            self.assertEqual(await peer.error(), (0, hs.E_BAD))
            self.assertEqual(await peer.recv(), (None, None))
        peer = await self.peer()
        await peer.create()
        peer.send(hs.SEND, struct.pack('<I', 0) + b'm' * (hs.MAX_PAYLOAD + 1))
        self.assertEqual(await peer.error(), (0, hs.E_BAD))
        await self.settled()
        self.assertEqual(self.server.rooms, {})

    async def test_rate_limit_ends_the_connection(self):
        self.listener.close()
        await self.start(max_delay=0.5)
        a = await self.peer()
        a.writer.write(frame(hs.PING) * 600)  # the last of them would wait 3 s
        pongs = 0
        kind, f = await a.recv()
        while kind == hs.PONG:
            pongs += 1
            kind, f = await a.recv()
        self.assertEqual((kind, f.u8(), f.u8()), (hs.ERROR, 0, hs.E_RATE))
        self.assertEqual(await a.recv(), (None, None))
        self.assertTrue(hs.BURST - 5 < pongs < hs.BURST + 150, pongs)  # the frames up to the bound were answered

    async def test_throttle_keeps_order(self):
        count = 560
        with mock.patch.object(hs, 'RATE', 200), self.assertLogs('hogs', 'INFO') as logs:
            host, *senders = [await self.peer() for _ in range(4)]
            room = await host.create()
            for sender in senders:
                await sender.join(room['id'])
            for _ in senders:
                await host.expect(hs.PLAYER_JOINED)
            started = time.monotonic()
            for number, sender in enumerate(senders):  # each one is held back after its first 300 frames
                sender.writer.write(b''.join(frame(hs.SEND, struct.pack('<IBI', host.id, number, i))
                                             for i in range(count)))
            got = [[] for _ in senders]
            for _ in range(count * len(senders)):
                f = await host.expect(hs.DATA)
                sender, to, (number, i) = f.u32(), f.u32(), struct.unpack('<BI', f.rest())
                self.assertEqual((sender, to), (senders[number].id, host.id))
                got[number].append(i)
            self.assertEqual(got, [list(range(count))] * len(senders))
            self.assertGreater(time.monotonic() - started, (count - hs.BURST) / hs.RATE * 0.9)
        self.assertEqual(len([line for line in logs.output if 'for the rate limit' in line]), len(senders))

    async def test_list_costs_more(self):
        with mock.patch.object(hs, 'RATE', 1000):
            a = await self.peer()
            started = time.monotonic()
            a.writer.write(frame(hs.LIST) * 25)  # the first 14 are free, the others are held back
            for _ in range(25):
                await a.expect(hs.ROOMS)
            self.assertGreater(time.monotonic() - started, 0.15)

    async def test_disconnect_is_logged(self):
        self.listener.close()
        await self.start(hello_seconds=0.2)
        with self.assertLogs('hogs', 'INFO') as logs:
            quiet, gone, bad = await self.peer(hello=False), await self.peer(), await self.peer()
            gone.writer.close()
            bad.send(hs.CREATE, bytes([0, 0]) + str8(b'x') + blob16(b'') + str8(b'p'))
            await bad.recv()
            self.assertEqual(await quiet.recv(), (None, None))
            await self.settled()
        text = '\n'.join(logs.output)
        for ident, cause in ((r'\d+', 'idle timeout'), (gone.id, 'the peer closed'),
                             (bad.id, 'bad frame: bad player limit')):
            self.assertRegex(text, r'127\.0\.0\.1 #%s gone: %s; \d+ s, \d+ frames, worst delay \d\.\d\d s, '
                             r'last frame \d+ s ago, \d+ bytes in, \d+ out' % (ident, cause))
        # HELLO is 13 bytes on the wire; the quiet one sent nothing and got nothing
        self.assertRegex(text, r'#%d gone: the peer closed; .* 1 frames, .* 13 bytes in, [1-9]\d* out' % gone.id)
        self.assertRegex(text, r'gone: idle timeout; .* 0 frames, .* 0 bytes in, 0 out')

    async def test_set_room_below_the_players(self):
        a, b, c = [await self.peer() for _ in range(3)]
        room = await a.create(limit=4)
        await b.join(room['id'])
        await c.join(room['id'])
        for peer, joins in (a, 2), (b, 1):
            for _ in range(joins):
                await peer.expect(hs.PLAYER_JOINED)
        a.send(hs.SET_ROOM, bytes([0, 1]) + blob16(b'd'))
        self.assertEqual((await a.expect(hs.OK)).u8(), hs.SET_ROOM)
        for peer in b, c:
            f = await peer.expect(hs.ROOM_CHANGED)
            self.assertEqual((f.u8(), f.u8()), (0, 3))
        self.assertEqual((await a.rooms())[0][0]['limit'], 3)
        for limit in 0, hs.MAX_PLAYERS + 1:  # these are still bad frames
            host = await self.peer()
            await host.create()
            host.send(hs.SET_ROOM, bytes([0, limit]) + blob16(b''))
            self.assertEqual(await host.error(), (0, hs.E_BAD))
            self.assertEqual(await host.recv(), (None, None))

    def test_patience(self):
        def patience(server, room, period):
            return server.patience(types.SimpleNamespace(compat=1, room=room, period=period))

        server = hs.Server()
        self.assertEqual([patience(server, True, p) for p in (0, 1, 5, 30)], [90, 10, 15, 90])
        self.assertEqual([patience(server, False, p) for p in (1, 30)], [90, 90])
        self.assertEqual(patience(hs.Server(idle_seconds=20), True, 30), 20)  # never later than the normal limit

    async def test_ping_period(self):
        self.listener.close()
        await self.start(idle_seconds=60, min_idle_seconds=0.1)  # a period of 1 second gives 3 seconds in a room
        told, left, silent, ignored = [await self.peer() for _ in range(4)]
        for peer in told, left, silent, ignored:
            await peer.create()
        left.send(hs.LEAVE)
        await left.expect(hs.OK)
        for peer, period in (told, 1), (left, 1), (silent, None), (ignored, 0), (ignored, 31):
            peer.send(hs.PING, bytes(4) + (bytes([period]) if period is not None else b''))
            self.assertEqual(len((await peer.expect(hs.PONG)).rest()), 4 + (period is not None))  # the echo is whole
        started = time.monotonic()
        self.assertEqual(await asyncio.wait_for(told.reader.read(), 6), b'')
        self.assertTrue(2.5 < time.monotonic() - started < 4.5)
        for peer in left, silent, ignored:  # no room, no period told, a period that is none: all still here
            peer.send(hs.PING, b'x')
            self.assertEqual((await peer.expect(hs.PONG)).rest(), b'x')

    async def test_front(self):
        self.listener.close()
        await self.start(front_secret='s3cret')
        a = await self.peer(front=(b's3cret', b'203.0.113.7'))
        self.assertEqual(a.first[0], hs.WELCOME)
        self.assertEqual([client.ip for client in self.server.clients], ['203.0.113.7'])

    async def test_front_refused(self):
        self.listener.close()
        await self.start(front_secret='s3cret')
        with self.assertLogs('hogs', 'INFO') as logs:
            for how in {'front': (b'wrong', b'203.0.113.7')}, {'front': (b'', b'203.0.113.7')}, {}:
                self.assertEqual((await self.peer(**how)).first, (None, None))  # closed, nothing was said
            short = await self.peer(hello=False)
            short.send(hs.FRONT)
            self.assertEqual(await short.recv(), (None, None))
            twice = await self.peer(hello=False)
            twice.send(hs.FRONT, str8(b's3cret') + str8(b'1.2.3.4'))
            twice.send(hs.FRONT, str8(b's3cret') + str8(b'1.2.3.4'))  # this one is no use after the first
            self.assertEqual(await twice.error(), (0, hs.E_BAD))
            await self.settled()
        text = '\n'.join(logs.output)
        self.assertIn('wrong front secret', text)
        self.assertIn('FRONT must come first', text)
        self.assertEqual(self.server.per_ip, {})

    async def test_front_is_a_bad_frame_without_a_secret(self):
        a = await self.peer(hello=False)
        a.send(hs.FRONT, str8(b'x') + str8(b'1.2.3.4'))
        self.assertEqual(await a.error(), (0, hs.E_BAD))
        b = await self.peer()
        b.send(hs.FRONT, str8(b'x') + str8(b'1.2.3.4'))
        self.assertEqual(await b.error(), (0, hs.E_BAD))

    async def test_limits_count_the_front_address(self):
        self.listener.close()
        await self.start(front_secret='k', max_per_ip=2)
        here, there = (b'k', b'198.51.100.1'), (b'k', b'198.51.100.2')
        a, b, c = await self.peer(front=here), await self.peer(front=here), await self.peer(front=there)
        refused = await self.peer(front=here, hello=False)
        self.assertEqual(await refused.error(), (0, hs.E_BUSY))
        self.assertEqual(await refused.recv(), (None, None))
        self.assertEqual(self.server.per_ip, {'198.51.100.1': 2, '198.51.100.2': 1})  # not the address of the peer
        a.writer.close()
        await self.settled()
        self.assertEqual(self.server.per_ip, {'198.51.100.1': 1, '198.51.100.2': 1})
        self.assertEqual((await self.peer(front=here)).first[0], hs.WELCOME)

    async def test_loopback_is_exempt(self):
        self.listener.close()
        await self.start(front_secret='k', max_clients=1, max_per_ip=1, free_loopback=True)
        for _ in range(3):
            self.assertEqual((await self.peer()).first[0], hs.WELCOME)  # no FRONT, no limit
        self.assertEqual(self.server.per_ip, {})
        self.assertEqual((await self.peer(front=(b'k', b'198.51.100.1'))).first[0], hs.WELCOME)  # it may send one
        self.assertEqual(self.server.per_ip, {'198.51.100.1': 1})  # and then it is counted by that address
        self.assertEqual((await self.peer(front=(b'x', b'198.51.100.1'))).first, (None, None))  # a wrong one is refused
        self.listener.close()
        await self.start(front_secret='k', max_clients=1, free_loopback=False)  # the total is checked on accept
        self.assertEqual((await self.peer(front=(b'k', b'198.51.100.1'))).first[0], hs.WELCOME)
        self.assertEqual(await (await self.peer(hello=False)).error(), (0, hs.E_BUSY))

    async def test_restart_notice(self):
        a, b, late = await self.peer(), await self.peer(), await self.peer(hello=False)
        await a.create()
        await self.server.close()
        for peer in a, b, late:
            f = await peer.expect(hs.ERROR)
            self.assertEqual((f.u8(), f.u8(), f.str8()), (0, hs.E_BUSY, b'the server restarts'))
            self.assertEqual(await peer.recv(), (None, None))


if __name__ == '__main__':
    unittest.main()
