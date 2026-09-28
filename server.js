// Checkmate Coach — lobby server.
//
// Serves the app's static files AND runs the lobby over the same HTTP
// server (so one deployed URL does both). Tracks who's connected, lists
// open games, relays lobby chat, and relays moves between two matched
// players. No database — everything lives in memory and resets if the
// server restarts, which is fine for a casual game lobby.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');

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
  // Guard against path traversal outside the public directory.
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
/** @type {Map<string, {id:string, hostId:string, hostName:string, hostWs:WebSocket, guestId:string|null, guestName:string|null, guestWs:WebSocket|null}>} */
const games = new Map();

function genId(len) {
  return crypto.randomBytes(len || 5).toString('hex').slice(0, len || 8);
}

function send(ws, msg) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function broadcastLobby() {
  const users = [...clients.values()].filter(c => c.ready).map(c => ({ id: c.id, name: c.name }));
  const openGames = [...games.values()]
    .filter(g => !g.guestId)
    .map(g => ({ id: g.id, hostName: g.hostName }));
  const payload = { t: 'lobby', users, games: openGames };
  for (const [ws, c] of clients) { if (c.ready) send(ws, payload); }
}

function findClientWs(id) {
  for (const [ws, c] of clients) { if (c.id === id) return ws; }
  return null;
}

function endGame(gameId, notifyOpponentOf) {
  const game = games.get(gameId);
  if (!game) return;
  games.delete(gameId);
  for (const id of [game.hostId, game.guestId]) {
    if (!id) continue;
    const c = [...clients.values()].find(cc => cc.id === id);
    if (c) c.gameId = null;
  }
  if (notifyOpponentOf) {
    const opponentId = notifyOpponentOf === game.hostId ? game.guestId : game.hostId;
    const ws = opponentId && findClientWs(opponentId);
    if (ws) send(ws, { t: 'opponentLeft' });
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
      const gid = genId(6);
      games.set(gid, { id: gid, hostId: c.id, hostName: c.name, hostWs: ws, guestId: null, guestName: null, guestWs: null });
      c.gameId = gid;
      send(ws, { t: 'hosted', id: gid });
      broadcastLobby();
      return;
    }

    if (msg.t === 'cancel') {
      if (c.gameId && games.has(c.gameId) && games.get(c.gameId).hostId === c.id && !games.get(c.gameId).guestId) {
        games.delete(c.gameId);
        c.gameId = null;
        broadcastLobby();
      }
      return;
    }

    if (msg.t === 'join') {
      const game = games.get(msg.id);
      if (!game) { send(ws, { t: 'error', message: 'That game no longer exists.' }); return; }
      if (game.guestId) { send(ws, { t: 'error', message: 'That game already has two players.' }); return; }
      if (c.gameId) { send(ws, { t: 'error', message: 'You are already in a game.' }); return; }
      if (game.hostId === c.id) { send(ws, { t: 'error', message: "You can't join your own game." }); return; }
      game.guestId = c.id; game.guestName = c.name; game.guestWs = ws;
      c.gameId = game.id;
      send(game.hostWs, { t: 'gameStart', color: 'w', opponent: c.name });
      send(ws, { t: 'gameStart', color: 'b', opponent: game.hostName });
      broadcastLobby();
      return;
    }

    if (msg.t === 'move') {
      if (!c.gameId) return;
      const game = games.get(c.gameId);
      if (!game) return;
      const opponentWs = game.hostId === c.id ? game.guestWs : game.hostWs;
      send(opponentWs, { t: 'move', from: msg.from, to: msg.to, promotion: msg.promotion || null });
      return;
    }

    if (msg.t === 'newgame') {
      if (!c.gameId) return;
      const game = games.get(c.gameId);
      if (!game) return;
      const opponentWs = game.hostId === c.id ? game.guestWs : game.hostWs;
      send(opponentWs, { t: 'newgame' });
      return;
    }

    if (msg.t === 'leave' || msg.t === 'resign') {
      if (!c.gameId) return;
      endGame(c.gameId, c.id);
      broadcastLobby();
      return;
    }
  });

  ws.on('close', () => {
    const c = clients.get(ws);
    if (c) {
      // Drop any game this client was hosting or playing.
      if (c.gameId) endGame(c.gameId, c.id);
      clients.delete(ws);
      broadcastLobby();
    }
  });
});

server.listen(PORT, () => {
  console.log('Checkmate Coach lobby server listening on port ' + PORT);
});
