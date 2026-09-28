# Checkmate Coach — Lobby Server

Runs the whole app: serves `public/index.html` (Checkmate Coach) and a
WebSocket lobby on the same port, so one deployed URL gives you both the
page and the multiplayer lobby (presence, chat, hosting/joining games,
move relay).

## Run locally

```bash
npm install
npm start
```

Then open http://localhost:3000 — the game's "Play online (lobby)" mode
will default its server field to `ws://localhost:3000`.

## Deploy on Render (free tier)

1. Push this folder to a GitHub repo.
2. On [render.com](https://render.com), New → Web Service → connect the repo.
3. Build command: `npm install`. Start command: `npm start`. Leave the
   region/plan as the free defaults.
4. Once deployed, Render gives you a URL like
   `https://checkmate-lobby.onrender.com`.
5. Open that URL — it serves the game directly, and "Play online (lobby)"
   will auto-fill the server field with the matching `wss://` address.

Render's free tier sleeps the service after inactivity, so the first
connection after a quiet period can take ~30 seconds to wake up — that's
normal, not a bug.

## Notes

- All lobby/game state lives in memory. Restarting the server clears the
  user list and any open/active games (players just get "disconnected" and
  can reconnect once it's back up).
- No accounts, no database — names are just what each player types in.
