// A Cloudflare Worker in front of the lobby server: the address players see.
//
// The game connects to this Worker with a WebSocket (wss://<worker>.<account>.workers.dev) and the
// Worker opens a TCP connection to the lobby server. Bytes pass both ways as they are: the lobby
// protocol of PROTOCOL.md runs inside, the Worker does not read it. A binary WebSocket message from
// the game is written to the server; what the server sends goes back as binary messages, cut where
// TCP cut it, so the game reads them as one stream of bytes.
//
// Setting (a secret of the Worker, not in this file):
//   ORIGIN   the lobby server, "host:port"
// To move the lobby to another server, change ORIGIN: the players' address stays the same.
import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request, env) {
    if (request.headers.get('Upgrade') !== 'websocket')
      return new Response('Hogs of War lobby\n');
    const at = env.ORIGIN.lastIndexOf(':');
    const origin = connect({ hostname: env.ORIGIN.slice(0, at), port: Number(env.ORIGIN.slice(at + 1)) });
    const writer = origin.writable.getWriter();
    const [client, server] = Object.values(new WebSocketPair());
    let open = true;
    const close = () => {
      if (!open) return;
      open = false;
      try { server.close(1000, 'closed'); } catch {}
      origin.close().catch(() => {});
    };

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
