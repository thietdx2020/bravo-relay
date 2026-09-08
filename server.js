/**
 * SwiftChat Zero-Storage Realtime WebSocket Relay Server
 * 
 * Đặc điểm:
 * - 100% In-Memory (Zero-Storage): Không có cơ sở dữ liệu, không ghi ổ đĩa.
 * - Giới hạn tối đa 2 thiết bị trong mỗi phòng (Slot 1 & Slot 2).
 * - Tự động xóa phòng ngay khi 2 máy thoát hoặc ngắt kết nối.
 */

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;

// Tạo HTTP Server phục vụ Health-Check cho Cloud (Render, Fly.io, Koyeb, Railway)
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('BRAVO KEY Relay Server is running.\nStatus: 200 OK\nMode: Zero-Storage RAM-Only\n');
});

const wss = new WebSocketServer({ server });

// Lưu trạng thái phòng trong RAM: roomCode -> Map of clientId -> { ws, senderName }
const rooms = new Map();

wss.on('connection', (ws) => {
    let currentRoom = null;
    let currentSenderId = null;

    ws.on('message', (data) => {
        try {
            const event = JSON.parse(data.toString());
            const { action, room, senderId, senderName, payload, timestamp } = event;

            if (action === 'join') {
                currentRoom = room;
                currentSenderId = senderId;

                if (!rooms.has(room)) {
                    rooms.set(room, new Map());
                }

                const roomParticipants = rooms.get(room);

                // Kiểm tra giới hạn 2 máy
                if (roomParticipants.size >= 2 && !roomParticipants.has(senderId)) {
                    ws.send(JSON.stringify({
                        action: 'room_full',
                        room: room,
                        senderId: 'system',
                        senderName: 'Hệ thống',
                        payload: 'Phòng đã đầy (chỉ dành cho tối đa 2 thiết bị)',
                        timestamp: Date.now() / 1000
                    }));
                    return;
                }

                // Lưu vào phòng trong RAM
                roomParticipants.set(senderId, { ws, senderName });
                console.log(`[+] Thiết bị "${senderName}" (${senderId}) vào phòng: ${room}. Số lượng: ${roomParticipants.size}/2`);

                // Nếu đã đủ 2 máy, gửi thông báo bắt đầu chat cho cả 2
                if (roomParticipants.size === 2) {
                    for (const [pId, pData] of roomParticipants.entries()) {
                        // Tìm người còn lại
                        let otherPeerName = 'Đối phương';
                        for (const [otherId, otherData] of roomParticipants.entries()) {
                            if (otherId !== pId) otherPeerName = otherData.senderName;
                        }

                        if (pData.ws.readyState === WebSocket.OPEN) {
                            pData.ws.send(JSON.stringify({
                                action: 'peer_joined',
                                room: room,
                                senderId: 'system',
                                senderName: otherPeerName,
                                payload: 'Đã kết nối thành công với đối phương',
                                timestamp: Date.now() / 1000
                            }));
                        }
                    }
                }
                return;
            }

            // Chuyển tiếp tin nhắn / sự kiện (message, typing, leave) tới máy còn lại
            if (currentRoom && rooms.has(currentRoom)) {
                const roomParticipants = rooms.get(currentRoom);
                for (const [pId, pData] of roomParticipants.entries()) {
                    if (pId !== senderId && pData.ws.readyState === WebSocket.OPEN) {
                        pData.ws.send(JSON.stringify(event));
                    }
                }
            }

            // Nếu người dùng chủ động gửi lệnh "leave" (Hủy phòng)
            if (action === 'leave') {
                handleUserLeave(currentRoom, currentSenderId);
                currentRoom = null;
                currentSenderId = null;
            }

        } catch (err) {
            console.error('[!] Lỗi xử lý message:', err.message);
        }
    });

    ws.on('close', () => {
        if (currentRoom && currentSenderId) {
            handleUserLeave(currentRoom, currentSenderId);
        }
    });

    function handleUserLeave(roomCode, senderId) {
        if (!roomCode || !rooms.has(roomCode)) return;

        const roomParticipants = rooms.get(roomCode);
        roomParticipants.delete(senderId);
        console.log(`[-] Thiết bị (${senderId}) đã rời phòng: ${roomCode}. Còn lại: ${roomParticipants.size}`);

        // Báo cho máy còn lại biết phòng đã bị hủy
        for (const [pId, pData] of roomParticipants.entries()) {
            if (pData.ws.readyState === WebSocket.OPEN) {
                pData.ws.send(JSON.stringify({
                    action: 'leave',
                    room: roomCode,
                    senderId: senderId,
                    senderName: 'Đối phương',
                    payload: 'Đối phương đã rời phòng hoặc hủy kết nối',
                    timestamp: Date.now() / 1000
                }));
            }
        }

        // Nếu không còn ai, xóa phòng khỏi RAM (Zero-Storage)
        if (roomParticipants.size === 0) {
            rooms.delete(roomCode);
            console.log(`[x] Phòng "${roomCode}" đã xóa sạch hoàn toàn khỏi RAM.`);
        }
    }
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 SwiftChat Zero-Storage Relay Server đang chạy tại cổng ${PORT}`);
});
