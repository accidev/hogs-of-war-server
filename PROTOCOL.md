# Wire protocol, version 1

How a game client talks to the lobby server. The client side is the fix for Hogs of War (`ddraw.dll`), which will stand in for DirectPlay; the last section says how the two map.

## Transport

One TCP connection per game client, port 47800 by default. All numbers are little-endian.

A frame is:

| Field | Size | |
|---|---|---|
| length | u16 | bytes that follow: 1 to 16384 |
| type | u8 | below |
| fields | length − 1 | |

Field kinds: `u8`, `u16`, `u32`; `str8` is a u8 length and that many bytes (at most 63); `blob16` is a u16 length and that many bytes (at most 1024); `bytes` is everything up to the end of the frame.

Names, descriptions, player data and messages are opaque bytes: the server stores them or passes them on and never looks inside. Text in `ERROR` and the server name are ASCII.

## Client to server

| Type | Name | Fields | Answer |
|---|---|---|---|
| 0x01 | HELLO | `'HOGS'`, u16 protocol, u32 compat | WELCOME, or ERROR and the connection closes |
| 0x02 | LIST | | ROOMS, one frame or several |
| 0x03 | CREATE | u8 max players, u8 flags, str8 room name, blob16 description, str8 player name | JOINED |
| 0x04 | JOIN | u32 room, str8 player name | JOINED |
| 0x05 | LEAVE | | OK |
| 0x06 | SEND | u32 to, bytes message (1 to 2048) | none |
| 0x07 | SET_ROOM | u8 flags, u8 max players, blob16 description | OK |
| 0x08 | SET_PLAYER | blob16 data | OK |
| 0x09 | PING | bytes (at most 16) | PONG with the same bytes |

- HELLO must be the first frame and must arrive within 10 seconds.
- `compat` is a number the client makes up from everything that must be equal for two games to stay in step: the version of the fix and the settings that change the battle. A room has the compat of its creator, and only clients with the same number can join it.
- `to` in SEND is a player id, or 0 for every other player of the room. A message to a player who has left goes nowhere, without an error.
- SET_ROOM is for the host. Flag bit 0 (`CLOSED`) stops new players from joining; the room stays in the list with the flag. Max players is 1 to 8 and not less than the players in the room.
- After 90 seconds without any frame from a client the server closes the connection: send PING when there is nothing else to send. WELCOME tells the number of seconds.

## Server to client

| Type | Name | Fields |
|---|---|---|
| 0x81 | WELCOME | u16 protocol, u32 your id, u16 max frame length, u16 idle seconds, str8 server name |
| 0x82 | ROOMS | u8 more, u16 count, then per room: u32 id, u32 compat, u8 flags, u8 players, u8 max players, str8 name, str8 host name, blob16 description |
| 0x83 | JOINED | u32 room, u32 host id, u8 flags, u8 max players, str8 name, blob16 description, u8 count, then per player: u32 id, str8 name, blob16 data |
| 0x84 | PLAYER_JOINED | u32 id, str8 name, blob16 data |
| 0x85 | PLAYER_LEFT | u32 id, u32 host id |
| 0x86 | DATA | u32 from, u32 to, bytes message |
| 0x87 | ROOM_CHANGED | u8 flags, u8 max players, blob16 description |
| 0x88 | PLAYER_DATA | u32 id, blob16 data |
| 0x89 | OK | u8 type of the request |
| 0x8A | ERROR | u8 type of the request (0: the connection), u8 code, str8 text |
| 0x8B | PONG | bytes |

- Your id from WELCOME is your player id in every room, for as long as the connection lives. Ids start at 0x100 and are not used twice.
- ROOMS with `more` = 1 is followed by another ROOMS frame; the list ends with `more` = 0.
- JOINED lists every player of the room, you included, in the order they joined. The first one is the host.
- When the host leaves, the player who joined next is the host: PLAYER_LEFT carries the id of the host after the leave. A room with no players is gone.
- `from` in DATA is set by the server: a client cannot send in another player's name. `to` is what the sender wrote.
- Frames the server sends because of other players (PLAYER_JOINED, PLAYER_LEFT, DATA, ROOM_CHANGED, PLAYER_DATA) can come between a request and its answer.

## Errors

| Code | Name | When | Connection |
|---|---|---|---|
| 1 | BAD | a frame that breaks these rules | closed |
| 2 | VERSION | HELLO with another protocol number | closed |
| 3 | NOT_FOUND | JOIN: no such room | stays |
| 4 | FULL | JOIN: the room is full | stays |
| 5 | CLOSED | JOIN: the room is closed | stays |
| 6 | COMPAT | JOIN: the room has another compat | stays |
| 7 | STATE | a request that needs a room without one, or CREATE and JOIN inside a room | stays |
| 8 | NOT_HOST | SET_ROOM from a player who is not the host | stays |
| 9 | BUSY | too many connections (in all, or from one address), or too many rooms | closed, except for CREATE |
| 10 | RATE | more than 100 frames a second for a while (300 at once are allowed) | closed |

A client that does not read what it is sent is dropped when 256 KB wait for it.

## For the DirectPlay side

The game uses these methods of `IDirectPlay` (found in `warhogs_.exe`, the network code around 0x460000):

| DirectPlay | Here |
|---|---|
| EnumConnections, InitializeConnection | the connection and HELLO |
| EnumSessions | LIST |
| Open (create), CreatePlayer | CREATE |
| Open (join), CreatePlayer | JOIN |
| EnumPlayers | the player list of JOINED, kept up to date with PLAYER_JOINED and PLAYER_LEFT |
| GetSessionDesc, SetSessionDesc | the room fields of JOINED, SET_ROOM, ROOM_CHANGED |
| SetPlayerData, GetPlayerData (the data is the player's team, 680 bytes) | SET_PLAYER, PLAYER_DATA |
| Send (the game's messages are 348 bytes) | SEND |
| Receive | DATA; the system messages of DirectPlay are made from PLAYER_JOINED, PLAYER_LEFT and the rest |
| DestroyPlayer, Close | LEAVE |

TCP keeps the order and loses nothing, so the "guaranteed" flag of Send needs no field.

The game names a new session "<Windows user name>'s Game". The client must put another name there: the room name is shown to everyone.

The server does not check messages. The client must check the size and the content of what it receives before the game reads it: another player's client can send anything.
