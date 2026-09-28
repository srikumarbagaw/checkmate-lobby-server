// Common game lobby server — Checkmate Coach (chess) + 304 Card Table.
//
// Serves the app's static files AND runs the lobby over the same HTTP
// server (so one deployed URL does both). Tracks who's connected, lists
// open games of any kind, relays lobby chat, and relays room messages
// between matched players. No database — everything lives in memory and
// resets if the server restarts, which is fine for a casual game lobby.
//
// The server is deliberately game-agnostic: beyond knowing each kind's
// seat capacity and whether it auto-starts when full, it just relays
// whatever JSON each game's own client code sends to the right room
// member(s). Chess and 304 each interpret their own message shapes.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');

const KIND_CONFIG = {
  chess: { capacity: 2, label: 'Chess' },
  '304': { capacity: 4, label: '304' },
};

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

const server = http.createServer((req, res) => {
  let urlPath = decodeURIComponent(req.url.split('?')[0]);
  if (urlPath === '/') urlPath = '/index.html';
  const filePath = path.join(PUBLIC_DIR, urlPath);
  if (!filePath.startsWith(PUBLIC_DIR)) { res.writeHead(403); res.end('Forbidden'); return; }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found'); return; }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });

/** @type {Map<WebSocket, {id:string, name:string, gameId:string|null, ready:boolean}>} */
const clients = new Map();
/** @type {Map<string, {id:string, kind:string, hostId:string, capacity:number, members:Map<string,string>}>} */
const games = new Map();

function genId(len) {
  return crypto.randomBytes(len || 5).toString('hex').slice(0, len || 8);
}

function send(ws, msg) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function findClientWs(id) {
  for (const [ws, c] of clients) { if (c.id === id) return ws; }
  return null;
}
function findClientRecord(id) {
  for (const c of clients.values()) { if (c.id === id) return c; }
  return null;
}

function broadcastLobby() {
  const users = [...clients.values()].filter(c => c.ready).map(c => ({ id: c.id, name: c.name }));
  const openGames = [...games.values()]
    .filter(g => g.members.size < g.capacity)
    .map(g => ({ id: g.id, hostName: g.members.get(g.hostId) || 'Player', kind: g.kind, count: g.members.size, capacity: g.capacity }));
  const payload = { t: 'lobby', users, games: openGames };
  for (const [ws, c] of clients) { if (c.ready) send(ws, payload); }
}

// Removes a client from whatever room it's in and tells the people who
// need to know. Host leaving closes the room for everyone (there's no
// authority left to run the game); a non-host leaving just vacates their
// seat and tells the host, who decides what happens next (chess ends the
// game; 304 lets the computer take over that seat).
function leaveGame(c) {
  const game = c.gameId && games.get(c.gameId);
  c.gameId = null;
  if (!game) return;
  if (game.hostId === c.id) {
    games.delete(game.id);
    for (const memberId of game.members.keys()) {
      if (memberId === c.id) continue;
      const mc = findClientRecord(memberId); if (mc) mc.gameId = null;
      send(findClientWs(memberId), { t: game.kind === 'chess' ? 'opponentLeft' : 'hostLeft' });
    }
  } else {
    game.members.delete(c.id);
    send(findClientWs(game.hostId), { t: game.kind === 'chess' ? 'opponentLeft' : 'memberLeft', id: c.id });
    if (game.kind === 'chess') {
      games.delete(game.id);
      const hc = findClientRecord(game.hostId); if (hc) hc.gameId = null;
    }
    // Other kinds (304): the room persists; the host manages the vacated seat.
  }
}

wss.on('connection', (ws) => {
  const id = genId(8);
  clients.set(ws, { id, name: 'Player', gameId: null, ready: false });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (err) { return; }
    const c = clients.get(ws);
    if (!c) return;

    if (msg.t === 'hello') {
      c.name = String(msg.name || 'Player').slice(0, 20) || 'Player';
      c.ready = true;
      send(ws, { t: 'welcome', id: c.id, name: c.name });
      broadcastLobby();
      return;
    }
    if (!c.ready) return; // ignore everything before hello

    if (msg.t === 'chat') {
      const text = String(msg.text || '').slice(0, 500).trim();
      if (!text) return;
      const payload = { t: 'chat', name: c.name, text, ts: Date.now() };
      for (const [otherWs, otherC] of clients) { if (otherC.ready) send(otherWs, payload); }
      return;
    }

    if (msg.t === 'host') {
      if (c.gameId) { send(ws, { t: 'error', message: 'You are already in a game.' }); return; }
      const kind = KIND_CONFIG[msg.kind] ? msg.kind : 'chess';
      const cfg = KIND_CONFIG[kind];
      const gid = genId(6);
      games.set(gid, { id: gid, kind, hostId: c.id, capacity: cfg.capacity, members: new Map([[c.id, c.name]]) });
      c.gameId = gid;
      send(ws, { t: 'hosted', id: gid, kind });
      broadcastLobby();
      return;
    }

    if (msg.t === 'cancel') {
      const g = c.gameId && games.get(c.gameId);
      if (g && g.hostId === c.id && g.members.size === 1) {
        games.delete(c.gameId);
        c.gameId = null;
        broadcastLobby();
      }
      return;
    }

    if (msg.t === 'join') {
      const game = games.get(msg.id);
      if (!game) { send(ws, { t: 'error', message: 'That game no longer exists.' }); return; }
      if (c.gameId) { send(ws, { t: 'error', message: 'You are already in a game.' }); return; }
      if (game.hostId === c.id) { send(ws, { t: 'error', message: "You can't join your own game." }); return; }
      if (game.members.size >= game.capacity) { send(ws, { t: 'error', message: 'That game is already full.' }); return; }
      game.members.set(c.id, c.name);
      c.gameId = game.id;
      if (game.kind === 'chess') {
        send(findClientWs(game.hostId), { t: 'gameStart', color: 'w', opponent: c.name });
        send(ws, { t: 'gameStart', color: 'b', opponent: game.members.get(game.hostId) });
      } else {
        send(ws, { t: 'joinedRoom', id: game.id, hostId: game.hostId, kind: game.kind, myId: c.id });
        send(findClientWs(game.hostId), { t: 'memberJoined', id: c.id, name: c.name });
      }
      broadcastLobby();
      return;
    }

    // Generic in-room relay: every game's own move/chat/state messages ride
    // on this, addressed to one member (`to`) or, if omitted, broadcast to
    // everyone else currently in the room.
    if (msg.t === 'relay') {
      if (!c.gameId) return;
      const game = games.get(c.gameId);
      if (!game) return;
      if (msg.to) {
        if (!game.members.has(msg.to)) return;
        send(findClientWs(msg.to), { t: 'relay', from: c.id, data: msg.data });
      } else {
        for (const memberId of game.members.keys()) {
          if (memberId === c.id) continue;
          send(findClientWs(memberId), { t: 'relay', from: c.id, data: msg.data });
        }
      }
      return;
    }

    if (msg.t === 'leave' || msg.t === 'resign') {
      if (!c.gameId) return;
      leaveGame(c);
      broadcastLobby();
      return;
    }
  });

  ws.on('close', () => {
    const c = clients.get(ws);
    if (c) {
      if (c.gameId) leaveGame(c);
      clients.delete(ws);
      broadcastLobby();
    }
  });
});

server.listen(PORT, () => {
  console.log('Common game lobby server listening on port ' + PORT);
});
