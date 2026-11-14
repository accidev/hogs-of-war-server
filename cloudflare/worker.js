// A Cloudflare Worker in front of the lobby server: the address players see.
//
// The game connects to this Worker with a WebSocket (wss://<worker>.<account>.workers.dev) and the
// Worker opens a TCP connection to the lobby server. Bytes pass both ways as they are: the lobby
// protocol of PROTOCOL.md runs inside, the Worker does not read it. A binary WebSocket message from
// the game is written to the server; what the server sends goes back as binary messages, cut where
// TCP cut it, so the game reads them as one stream of bytes.
//
// Any other request is for people: / is a page with the games online, /rooms.json is the list it reads.
// The Worker asks the lobby server for the list itself, and keeps the answer for 5 seconds.
//
// Settings (secrets of the Worker, not in this file):
//   ORIGIN        the lobby server, "host:port"
//   FRONT_SECRET  optional; the same value as HOGS_FRONT_SECRET of the server. When it is set, the first
//                 frame on every connection to the server is FRONT: the secret and the player's address.
// To move the lobby to another server, change ORIGIN: the players' address stays the same.
import { connect } from 'cloudflare:sockets';

const HELLO = 1, LIST = 2, FRONT = 0x0A, WELCOME = 0x81, ROOMS = 0x82, ERROR = 0x8A;
const MAX_FRAME = 16384, MAX_NAME = 63, ROOM_CLOSED = 1;
const encoder = new TextEncoder();

function frame(type, payload = []) {
  const out = new Uint8Array(3 + payload.length);
  out[0] = (payload.length + 1) & 255;
  out[1] = (payload.length + 1) >> 8;
  out[2] = type;
  out.set(payload, 3);
  return out;
}

function str8(text) {
  const bytes = encoder.encode(text);
  if (bytes.length > MAX_NAME) throw new RangeError('text too long for the protocol');
  return [bytes.length, ...bytes];
}

const frontFrame = (secret, address) => frame(FRONT, [...str8(secret), ...str8(address)]);

function concat(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}

// The first whole frame in the bytes read so far, or null; the bytes arrive cut anywhere.
function takeFrame(buffer) {
  if (buffer.length < 2) return null;
  const size = buffer[0] | (buffer[1] << 8);
  if (size < 1 || size > MAX_FRAME) throw new Error('bad frame size');
  if (buffer.length < 2 + size) return null;
  return { type: buffer[2], body: buffer.subarray(3, 2 + size), rest: buffer.subarray(2 + size) };
}

// Names are bytes from game clients: Latin-1 text without control characters, still not to be trusted.
const clean = (bytes) => String.fromCharCode(...bytes).replace(/[\x00-\x1f\x7f-\x9f]/g, '');

// The body of a ROOMS frame: u8 more, u16 count, then per room u32 id, u32 compat, u8 flags,
// u8 players, u8 max players, str8 name, str8 host name, blob16 description.
function parseRooms(body) {
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  let at = 3;
  const take = (count) => {
    if (at + count > body.length) throw new Error('short frame');
    at += count;
    return at - count;
  };
  const text = () => {
    const size = body[take(1)];
    return clean(body.subarray(take(size), at));
  };
  const rooms = [];
  for (let count = view.getUint16(1, true); count > 0; count--) {
    const head = take(11);
    const room = { compat: view.getUint32(head + 4, true), flags: body[head + 8],
                   players: body[head + 9], max: body[head + 10] };
    room.name = text();
    room.host = text();
    take(view.getUint16(take(2), true));
    rooms.push(room);
  }
  return { more: body[0], rooms };
}

// A room whose compat has bits above the lowest 16 has a password: it is counted, not listed.
function summarize(server, rooms) {
  const out = { server, players: 0, rooms: [], private: 0 };
  for (const room of rooms) {
    out.players += room.players;
    if (room.compat >>> 16) out.private++;
    else out.rooms.push({ name: room.name, host: room.host, players: room.players, max: room.max,
                          closed: Boolean(room.flags & ROOM_CLOSED), version: (room.compat >>> 8) & 0xFF });
  }
  return out;
}

function connectOrigin(env) {
  const at = env.ORIGIN.lastIndexOf(':');
  return connect({ hostname: env.ORIGIN.slice(0, at), port: Number(env.ORIGIN.slice(at + 1)) });
}

// The lobby's answer to LIST, as what /rooms.json says. The lobby takes the Worker for a front without
// an address, so it keeps the Worker's own.
async function askLobby(socket, env) {
  const writer = socket.writable.getWriter();
  const reader = socket.readable.getReader();
  let buffer = new Uint8Array(0);
  const next = async (type) => {
    for (;;) {
      const got = takeFrame(buffer);
      if (got) {
        buffer = got.rest;
        if (got.type !== type) throw new Error('the lobby answered with frame ' + got.type);
        return got.body;
      }
      const { value, done } = await reader.read();
      if (done) throw new Error('the lobby closed the connection');
      buffer = concat(buffer, value);
    }
  };
  const hello = frame(HELLO, [...encoder.encode('HOGS'), 1, 0, 0, 0, 0, 0]);
  await writer.write(env.FRONT_SECRET ? concat(frontFrame(env.FRONT_SECRET, ''), hello) : hello);
  const welcome = await next(WELCOME);
  const server = clean(welcome.subarray(11, 11 + welcome[10]));
  await writer.write(frame(LIST));
  const rooms = [];
  for (let more = 1; more;) {
    const got = parseRooms(await next(ROOMS));
    more = got.more;
    rooms.push(...got.rooms);
  }
  return summarize(server, rooms);
}

async function lobbyRooms(env) {
  const socket = connectOrigin(env);
  let timer;
  try {
    return await Promise.race([
      askLobby(socket, env),
      new Promise((_, fail) => { timer = setTimeout(() => fail(new Error('timeout')), 5000); }),
    ]);
  } finally {
    clearTimeout(timer);
    socket.close().catch(() => {});
  }
}

const json = (data, status = 200) => new Response(JSON.stringify(data), {
  status,
  headers: {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'public, max-age=5',
  },
});

// One answer for everybody for 5 seconds: the lobby is asked once, not once per viewer.
async function rooms(request, env, ctx) {
  const key = new URL('/rooms.json', request.url).href;
  const cached = await caches.default.match(key);
  if (cached) return cached;
  let response;
  try {
    response = json(await lobbyRooms(env));
  } catch (error) {
    console.error('rooms.json: ' + error);
    response = json({ error: 'the lobby does not answer' }, 502);
  }
  ctx.waitUntil(caches.default.put(key, response.clone()).catch(() => {}));
  return response;
}

const PAGE = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hogs of War — games online</title>
<style>
  :root { color-scheme: light dark; --bg: #fafaf7; --text: #202124; --dim: #666b70; --line: #d9d9d2; }
  @media (prefers-color-scheme: dark) { :root { --bg: #181a1b; --text: #e6e6e3; --dim: #9aa0a4; --line: #35393c; } }
  body { margin: 0; padding: 16px; background: var(--bg); color: var(--text); font: 16px/1.5 system-ui, sans-serif; }
  main { max-width: 42rem; margin: 0 auto; }
  h1 { font-size: 1.4rem; margin: 0 0 .5rem; }
  table { width: 100%; border-collapse: collapse; margin: .75rem 0; }
  th, td { padding: .4rem .75rem .4rem 0; text-align: left; vertical-align: top; overflow-wrap: anywhere;
           border-bottom: 1px solid var(--line); }
  th { color: var(--dim); font-size: .85rem; }
  .dim { color: var(--dim); }
</style>
</head>
<body>
<main>
<h1></h1>
<p id="online"></p>
<table hidden><thead><tr><th></th><th></th><th></th><th></th></tr></thead><tbody></tbody></table>
<p id="empty" class="dim" hidden></p>
<p id="private" class="dim" hidden></p>
<p id="error" hidden></p>
<p id="hint" class="dim"></p>
</main>
<script>
const TEXT = {
  en: {
    title: 'Hogs of War — games online', online: 'Players online: {n}',
    columns: ['Game', 'Host', 'Players', 'State'], open: 'open', battle: 'in battle',
    empty: 'No public games right now.', private: 'Private games: {n}',
    error: 'The list of games is not available right now.',
    hint: 'Games are joined from inside the game: Multi-Player, Network.',
  },
  ru: {
    title: 'Hogs of War — игры онлайн', online: 'Игроков онлайн: {n}',
    columns: ['Игра', 'Хозяин', 'Игроки', 'Состояние'], open: 'открыта', battle: 'идёт бой',
    empty: 'Открытых игр сейчас нет.', private: 'Закрытых паролем игр: {n}',
    error: 'Список игр сейчас недоступен.',
    hint: 'К играм присоединяются из самой игры: «Много игроков» → «Сеть».',
  },
};
const lang = (navigator.language || '').toLowerCase().startsWith('ru') ? 'ru' : 'en';
const t = TEXT[lang];
const $ = (selector) => document.querySelector(selector);

document.documentElement.lang = lang;
document.title = t.title;
$('h1').textContent = t.title;
$('#hint').textContent = t.hint;
document.querySelectorAll('th').forEach((th, i) => { th.textContent = t.columns[i]; });

// Names come from other players: they are put in as text and never as HTML.
function show(data) {
  const rows = $('tbody');
  rows.textContent = '';
  for (const room of data.rooms) {
    const row = rows.insertRow();
    for (const text of [room.name, room.host, room.players + ' / ' + room.max, room.closed ? t.battle : t.open])
      row.insertCell().textContent = text;
  }
  $('#online').textContent = t.online.replace('{n}', data.players);
  $('#online').hidden = false;
  $('table').hidden = !data.rooms.length;
  $('#empty').textContent = t.empty;
  $('#empty').hidden = data.rooms.length > 0;
  $('#private').textContent = t.private.replace('{n}', data.private);
  $('#private').hidden = !data.private;
  $('#error').hidden = true;
}

function fail() {
  for (const selector of ['#online', 'table', '#empty', '#private']) $(selector).hidden = true;
  $('#error').textContent = t.error;
  $('#error').hidden = false;
}

async function refresh() {
  try {
    const response = await fetch('/rooms.json');
    if (!response.ok) throw new Error('status ' + response.status);
    show(await response.json());
  } catch (error) {
    fail();
  }
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
`;

export default {
  async fetch(request, env, ctx) {
    if ((request.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
      if (new URL(request.url).pathname === '/rooms.json') return rooms(request, env, ctx);
      return new Response(PAGE, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
    }
    const origin = connectOrigin(env);
    const writer = origin.writable.getWriter();
    const [client, server] = Object.values(new WebSocketPair());
    let open = true;
    const close = () => {
      if (!open) return;
      open = false;
      try { server.close(1000, 'closed'); } catch {}
      origin.close().catch(() => {});
    };

    if (env.FRONT_SECRET)
      writer.write(frontFrame(env.FRONT_SECRET, request.headers.get('CF-Connecting-IP') || '')).catch(close);
    server.binaryType = 'arraybuffer';
    server.accept();
    server.addEventListener('message', (event) => {
      if (typeof event.data === 'string') return close(); // the game sends bytes only
      writer.write(new Uint8Array(event.data)).catch(close);
    });
    server.addEventListener('close', close);
    server.addEventListener('error', close);
    (async () => {
      try {
        for await (const chunk of origin.readable) server.send(chunk);
      } catch {}
      close();
    })();
    return new Response(null, { status: 101, webSocket: client });
  },
};
