// Drift City - dependency-free WebSocket relay server (Node 18+)
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_PER_ROOM = 16;
const rooms = new Map();
const meta = new Map(); // room -> { mode, track, host }
let nextId = 1;

const server = http.createServer((req, res) => {
  let n = 0;
  for (const r of rooms.values()) n += r.size;
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' });
  res.end('Drift City server OK - players online: ' + n + ' - rooms: ' + rooms.size);
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const c = { id: nextId++, sock, room: null, name: '', col: 0, b: 0, buf: Buffer.alloc(0), alive: true, last: 0, dropped: false };
  sock.setNoDelay(true);
  sock.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    if (c.buf.length > 65536) { sock.destroy(); return; }
    parse(c);
  });
  sock.on('close', () => drop(c));
  sock.on('error', () => drop(c));
});

function parse(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const op = b[0] & 15, masked = b[1] & 128;
    let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    if (len > 16384) { c.sock.destroy(); return; }
    const mk = masked ? 4 : 0;
    if (b.length < off + mk + len) return;
    const p = Buffer.from(b.subarray(off + mk, off + mk + len));
    if (masked) { const m = b.subarray(off, off + 4); for (let i = 0; i < p.length; i++) p[i] ^= m[i & 3]; }
    c.buf = b.subarray(off + mk + len);
    c.alive = true;
    if (op === 8) { try { c.sock.end(); } catch (e) {} return; }
    if (op === 9) { send(c, p, 10); continue; }
    if (op === 1) onMsg(c, p.toString('utf8'));
  }
}

function send(c, data, op = 1) {
  if (c.sock.destroyed) return;
  const p = Buffer.isBuffer(data) ? data : Buffer.from(data);
  let h;
  if (p.length < 126) h = Buffer.from([128 | op, p.length]);
  else if (p.length < 65536) { h = Buffer.alloc(4); h[0] = 128 | op; h[1] = 126; h.writeUInt16BE(p.length, 2); }
  else { h = Buffer.alloc(10); h[0] = 128 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(p.length), 2); }
  try { c.sock.write(Buffer.concat([h, p])); } catch (e) {}
}

const pub = c => ({ id: c.id, name: c.name, col: c.col, b: c.b });
const num = v => (Number.isFinite(v) ? Math.max(-1e6, Math.min(1e6, v)) : 0);
function bc(room, except, obj) {
  if (!room) return;
  const s = JSON.stringify(obj);
  for (const o of room.values()) if (o !== except) send(o, s);
}

function onMsg(c, text) {
  let m;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  if (m.t === 'join') {
    if (c.room) return;
    const rn = String(m.room || 'main').slice(0, 20).replace(/[^\w\u0600-\u06FF-]/g, '') || 'main';
    const room = rooms.get(rn) || new Map();
    if (room.size >= MAX_PER_ROOM) { send(c, JSON.stringify({ t: 'full' })); return; }
    rooms.set(rn, room);
    if (!room.size) meta.set(rn, { mode: m.mode ? 1 : 0, track: (m.track | 0) % 8, host: c.id });
    c.room = rn;
    c.name = String(m.name || 'لاعب').slice(0, 14);
    c.col = (m.col >>> 0) & 0xffffff;
    c.b = m.b ? 1 : 0;
    room.set(c.id, c);
    const mt = meta.get(rn);
    send(c, JSON.stringify({ t: 'w', id: c.id, room: rn, mode: mt.mode, track: mt.track, host: mt.host, players: [...room.values()].filter(o => o !== c).map(pub) }));
    bc(room, c, { t: 'j', p: pub(c) });
  } else if (m.t === 'go' && c.room) {
    const mt = meta.get(c.room);
    if (mt && mt.host === c.id) bc(rooms.get(c.room), null, { t: 'go', track: mt.track });
  } else if (m.t === 's' && c.room) {
    const now = Date.now();
    if (now - c.last < 50) return;
    c.last = now;
    c.b = m.b ? 1 : 0;
    bc(rooms.get(c.room), c, { t: 's', id: c.id, x: num(m.x), z: num(m.z), y: num(m.y), r: num(m.r), v: num(m.v), b: c.b, g: num(m.g), f: m.f ? 1 : 0, ft: num(m.ft) });
  }
}

function drop(c) {
  if (c.dropped) return;
  c.dropped = true;
  if (c.room) {
    const room = rooms.get(c.room);
    if (room) {
      room.delete(c.id);
      bc(room, null, { t: 'l', id: c.id });
      const mt = meta.get(c.room);
      if (!room.size) { rooms.delete(c.room); meta.delete(c.room); }
      else if (mt && mt.host === c.id) { mt.host = room.keys().next().value; bc(room, null, { t: 'h', id: mt.host }); }
    }
  }
  try { c.sock.destroy(); } catch (e) {}
}

setInterval(() => {
  for (const room of rooms.values()) {
    for (const c of [...room.values()]) {
      if (!c.alive) { drop(c); continue; }
      c.alive = false;
      send(c, Buffer.alloc(0), 9);
    }
  }
}, 20000);

server.listen(PORT, () => console.log('Drift City server listening on ' + PORT));
