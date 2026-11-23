const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png'
};

function sendFile(res, filePath) {
  const safePath = path.normalize(filePath);
  if (!safePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }
  fs.readFile(safePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not Found');
      return;
    }
    const ext = path.extname(safePath).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'no-store, no-cache, must-revalidate'
    });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    res.end();
    return;
  }

  const reqPath = (req.url || '/').split('?')[0];

  if (reqPath === '/health') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'no-store'
    });
    res.end(JSON.stringify({ ok: true, version: '1.1.0' }));
    return;
  }

  if (reqPath === '/config.json') {
    sendFile(res, path.join(PUBLIC_DIR, 'config.json'));
    return;
  }

  if (reqPath.startsWith('/ui/')) {
    const rel = reqPath.slice('/ui/'.length);
    sendFile(res, path.join(PUBLIC_DIR, rel));
    return;
  }

  res.writeHead(200, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Access-Control-Allow-Origin': '*'
  });
  res.end('Hazan Watch Party Sunucusu Aktif (v1.1).');
});

const wss = new WebSocketServer({ server });

// rooms: Map<roomId, { videoId: string|null, clients: Set<client>, adClients: Set<string>, adPausedRoom: boolean }>
const rooms = new Map();

function broadcast(roomId, payload, excludeClient = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const data = JSON.stringify(payload);
  for (const client of room.clients) {
    if (client !== excludeClient && client.ws.readyState === 1) {
      client.ws.send(data);
    }
  }
}

function sendUserList(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  const users = Array.from(room.clients).map((c) => ({ id: c.id, username: c.username }));
  broadcast(roomId, { type: 'USER_LIST', users });
}

function requestSyncFromPeer(roomId, requesterClient) {
  const room = rooms.get(roomId);
  if (!room) return;
  const peer = Array.from(room.clients).find(
    (c) =>
      c !== requesterClient &&
      c.ws.readyState === 1 &&
      c.ws.isAlive !== false &&
      (!room.adClients || !room.adClients.has(c.id))
  );
  if (peer) {
    peer.ws.send(JSON.stringify({
      type: 'REQUEST_STATE',
      targetClientId: requesterClient.id
    }));
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  const client = {
    id: Math.random().toString(36).substring(2, 10),
    ws,
    roomId: null,
    username: 'Misafir'
  };

  ws.on('message', (raw) => {
    ws.isAlive = true;
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === 'PING') {
      if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'PONG' }));
      return;
    }

    if (msg.type === 'LOOKUP') {
      const roomId = String(msg.roomId || '').trim().toUpperCase();
      const room = rooms.get(roomId);
      ws.send(JSON.stringify({
        type: 'LOOKUP_RESULT',
        roomId,
        exists: Boolean(room),
        videoId: room ? room.videoId : null
      }));
      return;
    }

    if (msg.type === 'JOIN') {
      const roomId = String(msg.roomId || '').trim().toUpperCase();
      const username = String(msg.username || 'Misafir').trim().slice(0, 24);
      const videoId = msg.videoId ? String(msg.videoId).trim() : null;
      if (!roomId) return;

      if (client.roomId && client.roomId !== roomId) {
        const prevRoomId = client.roomId;
        const prevRoom = rooms.get(prevRoomId);
        if (prevRoom) {
          prevRoom.clients.delete(client);
          if (prevRoom.adClients) prevRoom.adClients.delete(client.id);
          if (prevRoom.clients.size === 0) {
            rooms.delete(prevRoomId);
          } else {
            if (prevRoom.adPausedRoom && (!prevRoom.adClients || prevRoom.adClients.size === 0)) {
              prevRoom.adPausedRoom = false;
              broadcast(prevRoomId, {
                type: 'AD_RESUME',
                from: client.username,
                endedByClientId: client.id
              });
            }
            sendUserList(prevRoomId);
          }
        }
      }

      client.roomId = roomId;
      client.username = username;

      if (!rooms.has(roomId)) {
        rooms.set(roomId, { videoId: videoId, clients: new Set(), adClients: new Set(), adPausedRoom: false });
      }
      const room = rooms.get(roomId);
      if (!room.adClients) room.adClients = new Set();
      if (!room.videoId && videoId) {
        room.videoId = videoId;
      }

      room.clients.add(client);

      ws.send(JSON.stringify({
        type: 'JOINED',
        clientId: client.id,
        roomId,
        username,
        videoId: room.videoId
      }));

      if (room.adClients.size > 0) {
        const waitingFor = Array.from(room.clients)
          .filter((c) => room.adClients.has(c.id))
          .map((c) => c.username);
        ws.send(JSON.stringify({ type: 'AD_WAIT', waitingFor }));
      }

      broadcast(roomId, {
        type: 'SYSTEM',
        text: `${username} odaya katildi.`
      });

      sendUserList(roomId);
      return;
    }

    if (!client.roomId) return;
    const room = rooms.get(client.roomId);
    if (!room) return;

    if (msg.type === 'AD_STATUS') {
      const inAd = Boolean(msg.inAd);
      if (!room.adClients) room.adClients = new Set();

      if (inAd) {
        if (room.adClients.has(client.id)) return;
        const wasEmpty = room.adClients.size === 0;
        room.adClients.add(client.id);
        room.adPausedRoom = true;
        if (wasEmpty) {
          broadcast(client.roomId, {
            type: 'AD_PAUSE',
            from: client.username,
            adCount: room.adClients.size
          }, client);
        } else {
          broadcast(client.roomId, {
            type: 'AD_NOTICE',
            from: client.username,
            adCount: room.adClients.size
          }, client);
        }
      } else {
        if (!room.adClients.has(client.id)) return;
        room.adClients.delete(client.id);
        if (room.adClients.size > 0) {
          const waitingFor = Array.from(room.clients)
            .filter((c) => room.adClients.has(c.id))
            .map((c) => c.username);
          if (ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'AD_WAIT', waitingFor }));
          }
        } else if (room.adPausedRoom) {
          room.adPausedRoom = false;
          broadcast(client.roomId, {
            type: 'AD_RESUME',
            from: client.username,
            endedByClientId: client.id
          });
        }
      }
      return;
    }

    if (msg.type === 'REQUEST_SYNC') {
      requestSyncFromPeer(client.roomId, client);
      return;
    }

    if (msg.type === 'STATE_RESPONSE') {
      const target = Array.from(room.clients).find((c) => c.id === msg.targetClientId);
      if (target && target.ws.readyState === 1) {
        target.ws.send(JSON.stringify({
          type: 'SYNC',
          action: msg.paused ? 'pause' : 'play',
          time: msg.time,
          paused: msg.paused,
          from: client.username,
          isInitialSync: true
        }));
      }
      return;
    }

    if (msg.type === 'CHANGE_VIDEO') {
      const newVideoId = String(msg.videoId || '').trim();
      if (!newVideoId || newVideoId === room.videoId) return;
      room.videoId = newVideoId;
      broadcast(client.roomId, {
        type: 'CHANGE_VIDEO',
        videoId: newVideoId,
        from: client.username
      }, client);
      return;
    }

    if (msg.type === 'SYNC') {
      broadcast(client.roomId, {
        type: 'SYNC',
        action: msg.action,
        time: msg.time,
        paused: msg.paused,
        from: client.username
      }, client);
      return;
    }

    if (msg.type === 'TYPING') {
      broadcast(client.roomId, {
        type: 'TYPING',
        from: client.username,
        typing: Boolean(msg.typing)
      }, client);
      return;
    }

    if (msg.type === 'CHAT') {
      const text = String(msg.text || '').trim().slice(0, 500);
      if (!text) return;
      let replyTo = null;
      if (msg.replyTo && typeof msg.replyTo === 'object') {
        const rText = String(msg.replyTo.text || '').trim().slice(0, 140);
        if (rText) {
          replyTo = {
            id: msg.replyTo.id ? String(msg.replyTo.id).slice(0, 32) : null,
            from: String(msg.replyTo.from || 'Misafir').trim().slice(0, 24),
            text: rText
          };
        }
      }
      const now = new Date();
      const msgId = now.getTime().toString(36) + '-' + Math.random().toString(36).substring(2, 7);
      const timeStr = now.toLocaleTimeString('tr-TR', {
        timeZone: 'Europe/Istanbul',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false
      });
      broadcast(client.roomId, {
        type: 'CHAT',
        id: msgId,
        senderId: client.id,
        from: client.username,
        text,
        replyTo,
        timeStr,
        ts: now.getTime()
      });
      return;
    }
  });

  ws.on('close', () => {
    if (!client.roomId) return;
    const room = rooms.get(client.roomId);
    if (!room) return;
    room.clients.delete(client);
    const wasInAd = room.adClients ? room.adClients.delete(client.id) : false;
    if (room.clients.size === 0) {
      rooms.delete(client.roomId);
    } else {
      broadcast(client.roomId, {
        type: 'SYSTEM',
        text: `${client.username} odadan ayrildi.`
      });
      if (wasInAd && room.adPausedRoom && (!room.adClients || room.adClients.size === 0)) {
        room.adPausedRoom = false;
        broadcast(client.roomId, {
          type: 'AD_RESUME',
          from: client.username,
          endedByClientId: client.id
        });
      }
      sendUserList(client.roomId);
    }
  });
});

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  });
}, 30000);

wss.on('close', () => clearInterval(heartbeatInterval));

server.listen(PORT, () => {
  console.log(`Hazan Sunucusu v1.1 calisiyor -> ws://localhost:${PORT}`);
});