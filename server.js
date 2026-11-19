const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 2;
const RELAY_TYPES = new Set(['offer', 'answer', 'ice-candidate']);

// roomId -> { host: ws, guest: ws | null }
const rooms = new Map();

// Plain HTTP endpoint: lets hosting platforms and uptime monitors check the server
const server = http.createServer((req, res) => {
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', rooms: rooms.size, online: onlineCount() }));
    } else {
        res.writeHead(404);
        res.end();
    }
});
const wss = new WebSocket.Server({ server });

// ---------- helpers ----------
function send(ws, obj) {
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(obj) {
    for (const client of wss.clients) send(client, obj);
}

function onlineCount() {
    let n = 0;
    for (const c of wss.clients) if (c.readyState === WebSocket.OPEN) n++;
    return n;
}

function openRooms() {
    return [...rooms].filter(([, r]) => !r.guest).map(([id]) => id);
}

function broadcastRoomList() {
    broadcast({ type: 'room-list', rooms: openRooms() });
}

function broadcastServerInfo() {
    broadcast({ type: 'server-info', online: onlineCount() });
}

function sendRoomStatus(id) {
    const room = rooms.get(id);
    if (!room) return;
    const msg = {
        type: 'room-status',
        roomId: id,
        players: room.guest ? 2 : 1,
        max: MAX_PLAYERS
    };
    send(room.host, msg);
    send(room.guest, msg);
}

function leaveRoom(ws) {
    const id = ws.roomId;
    const room = rooms.get(id);
    const wasHost = ws.isHost;
    ws.roomId = null;
    ws.isHost = false;
    if (!room) return;

    if (wasHost) {
        send(room.guest, { type: 'room-closed' });
        if (room.guest) room.guest.roomId = null;
        rooms.delete(id);
        console.log(`Host left. Room destroyed: ${id}`);
    } else if (room.guest === ws) {
        room.guest = null;
        send(room.host, { type: 'peer-left' });
        sendRoomStatus(id);
        console.log(`Guest left room: ${id}`);
    }
    broadcastRoomList();
}

// ---------- connections ----------
wss.on('connection', (ws) => {
    ws.roomId = null;
    ws.isHost = false;
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    send(ws, { type: 'room-list', rooms: openRooms() });
    broadcastServerInfo();

    ws.on('message', (raw) => {
        let data;
        try { data = JSON.parse(raw.toString()); } catch { return; }

        switch (data.type) {
            case 'create-room': {
                const id = String(data.roomId ?? '').trim();
                if (!id || id.length > 32) return send(ws, { type: 'error', message: 'Invalid room name.' });
                if (ws.roomId) return send(ws, { type: 'error', message: 'You are already in a room.' });
                if (rooms.has(id)) return send(ws, { type: 'error', message: 'That room name is already taken.' });

                rooms.set(id, { host: ws, guest: null });
                ws.roomId = id;
                ws.isHost = true;
                send(ws, { type: 'room-created', roomId: id });
                sendRoomStatus(id);
                broadcastRoomList();
                console.log(`Room created: ${id}`);
                break;
            }

            case 'join-room': {
                if (ws.roomId) return send(ws, { type: 'error', message: 'You are already in a room.' });
                const room = rooms.get(data.roomId);
                if (!room) return send(ws, { type: 'error', message: 'Room no longer exists.' });
                if (room.guest) return send(ws, { type: 'error', message: 'Room is full.' });

                room.guest = ws;
                ws.roomId = data.roomId;
                ws.isHost = false;
                send(ws, { type: 'room-joined', roomId: data.roomId });
                send(room.host, { type: 'peer-joined' });
                sendRoomStatus(data.roomId);
                broadcastRoomList(); // room is full now, hide it
                console.log(`Peer joined room: ${data.roomId}`);
                break;
            }

            case 'leave-room':
                leaveRoom(ws);
                break;

            default: {
                if (!RELAY_TYPES.has(data.type)) return;
                const room = rooms.get(ws.roomId);
                if (!room) return;
                send(ws.isHost ? room.guest : room.host, data);
            }
        }
    });

    ws.on('close', () => {
        leaveRoom(ws);
        broadcastServerInfo();
    });
    ws.on('error', (e) => console.error('Socket error:', e.message));
});

// Heartbeat: drop connections that stopped answering (close doesn't always fire on a network drop)
setInterval(() => {
    for (const ws of wss.clients) {
        if (!ws.isAlive) { ws.terminate(); continue; }
        ws.isAlive = false;
        ws.ping();
    }
}, 30000);

server.listen(PORT, () => console.log(`Signaling server running on port ${PORT}`));