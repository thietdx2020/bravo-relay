/**
 * SwiftChat Zero-Storage Realtime WebSocket Relay Server
 * 
 * Đặc điểm:
 * - 100% In-Memory (Zero-Storage): Không có cơ sở dữ liệu, không ghi ổ đĩa.
 * - Giới hạn tối đa 2 thiết bị trong mỗi phòng (Slot 1 & Slot 2).
 * - Heartbeat Ping/Pong 15s chống Cloudflare/4G idle timeout.
 * - Hộp thư tin nhắn mã hoá bất đồng bộ (E2EE Dead-Drop Mailbox).
 * - Cơ chế Xoá 2 Chiều (Delete for Everyone & Clear History) chuẩn Telegram.
 * - Tự động dọn dẹp tin nhắn quá 24h.
 */

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;

// Tạo HTTP Server phục vụ Health-Check cho Cloud (Render, Fly.io, Koyeb, Railway)
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('BRAVO KEY Relay Server is running.\nStatus: 200 OK\nMode: Zero-Storage RAM-Only\nHeartbeat: 15s\nMailbox: E2EE Dead-Drop\n');
});

const wss = new WebSocketServer({ server });

// Lưu trạng thái phòng trong RAM: roomCode -> Map of senderId -> { ws, senderName }
const rooms = new Map();

// Hộp thư tin nhắn mã hoá bất đồng bộ theo phòng (E2EE Dead-Drop Mailbox)
// roomCode -> Array of { messageId, senderId, senderName, payload, timestamp }
const roomMailboxes = new Map();

// Tự động dọn dẹp các tin nhắn quá 24h trong Hộp thư mỗi 15 phút
const PURGE_TTL_SECONDS = 24 * 3600;
setInterval(() => {
    const nowSec = Date.now() / 1000;
    for (const [rCode, mList] of roomMailboxes.entries()) {
        const validMessages = mList.filter(m => (nowSec - m.timestamp) < PURGE_TTL_SECONDS);
        if (validMessages.length === 0) {
            roomMailboxes.delete(rCode);
        } else {
            roomMailboxes.set(rCode, validMessages);
        }
    }
}, 15 * 60 * 1000);

// Helper: Dọn dẹp các socket chết / đã đóng khỏi phòng
function pruneRoom(roomCode) {
    if (!rooms.has(roomCode)) return;
    const participants = rooms.get(roomCode);
    for (const [sId, data] of participants.entries()) {
        if (!data.ws || data.ws.readyState !== WebSocket.OPEN) {
            participants.delete(sId);
            console.log(`[clean] Đã dọn dẹp socket chết của (${sId}) trong phòng: ${roomCode}`);
        }
    }
    if (participants.size === 0) {
        rooms.delete(roomCode);
        console.log(`[x] Phòng rỗng "${roomCode}" đã giải phóng socket.`);
    }
}

// Helper: Phát thông báo peer_joined cho cả 2 máy trong phòng
function broadcastPeerJoined(roomCode) {
    if (!rooms.has(roomCode)) return;
    const participants = rooms.get(roomCode);
    if (participants.size !== 2) return;

    for (const [pId, pData] of participants.entries()) {
        let otherPeerName = 'Đối phương';
        for (const [otherId, otherData] of participants.entries()) {
            if (otherId !== pId) otherPeerName = otherData.senderName;
        }

        if (pData.ws.readyState === WebSocket.OPEN) {
            pData.ws.send(JSON.stringify({
                action: 'peer_joined',
                room: roomCode,
                senderId: 'system',
                senderName: otherPeerName,
                payload: 'Đã kết nối thành công với đối phương',
                timestamp: Date.now() / 1000
            }));
        }
    }
}

// Server-side Heartbeat Ping/Pong (15 giây)
// Giúp Cloudflare và trạm phát sóng 4G duy trì kết nối liên tục, đồng thời phát hiện socket chết
const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log(`[heartbeat] Socket không phản hồi pong -> terminate`);
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 15000);

wss.on('close', () => {
    clearInterval(heartbeatInterval);
});

wss.on('connection', (ws) => {
    ws.isAlive = true;
    let currentRoom = null;
    let currentSenderId = null;

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (data) => {
        try {
            const event = JSON.parse(data.toString());
            const { action, room, senderId, senderName, messageId, payload, timestamp } = event;

            if (action === 'join') {
                currentRoom = room;
                currentSenderId = senderId;

                if (!rooms.has(room)) {
                    rooms.set(room, new Map());
                }

                // Trước khi kiểm tra số lượng, dọn dẹp các socket đã đóng trong phòng
                pruneRoom(room);
                const roomParticipants = rooms.get(room) || new Map();
                rooms.set(room, roomParticipants);

                // Nếu senderId này đã có socket trước đó trong phòng (vừa rớt mạng và reconnect lại)
                if (roomParticipants.has(senderId)) {
                    const oldData = roomParticipants.get(senderId);
                    if (oldData.ws !== ws) {
                        console.log(`[reconnect] Thay thế socket cũ của "${senderName}" (${senderId})`);
                        try {
                            oldData.ws.removeAllListeners('close');
                            oldData.ws.terminate();
                        } catch (e) {}
                    }
                } else if (roomParticipants.size >= 2) {
                    // Phòng đã có 2 máy khác đang kết nối thật
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

                // Đăng ký socket vào phòng
                roomParticipants.set(senderId, { ws, senderName });
                console.log(`[+] Thiết bị "${senderName}" (${senderId}) vào phòng: ${room}. Số lượng: ${roomParticipants.size}/2`);

                // ĐỒNG BỘ LỊCH SỬ TIN NHẮN TỪ HỘP THƯ CHO THIẾT BỊ MỚI VÀO
                if (roomMailboxes.has(room)) {
                    const mailbox = roomMailboxes.get(room);
                    if (mailbox && mailbox.length > 0) {
                        ws.send(JSON.stringify({
                            action: 'history_sync',
                            room: room,
                            senderId: 'system',
                            senderName: 'Hệ thống',
                            payload: JSON.stringify(mailbox),
                            timestamp: Date.now() / 1000
                        }));
                        console.log(`[sync] Đã gửi ${mailbox.length} tin nhắn lịch sử cho "${senderName}"`);
                    }
                }

                // Nếu đã đủ 2 máy, lập tức đồng bộ peer_joined cho cả 2
                if (roomParticipants.size === 2) {
                    broadcastPeerJoined(room);
                }
                return;
            }

            // XỬ LÝ LỆNH XOÁ TỪNG TIN NHẮN 2 CHIỀU (Delete for Everyone)
            if (action === 'delete_message') {
                const targetId = messageId || payload;
                if (currentRoom && roomMailboxes.has(currentRoom)) {
                    const mailbox = roomMailboxes.get(currentRoom);
                    const filtered = mailbox.filter(m => m.messageId !== targetId);
                    roomMailboxes.set(currentRoom, filtered);
                    console.log(`[delete_msg] Đã xoá tin (${targetId}) khỏi hộp thư phòng: ${currentRoom}`);
                }
                // Chuyển tiếp lệnh xoá tới máy đối phương
                if (currentRoom && rooms.has(currentRoom)) {
                    const roomParticipants = rooms.get(currentRoom);
                    for (const [pId, pData] of roomParticipants.entries()) {
                        if (pId !== senderId && pData.ws.readyState === WebSocket.OPEN) {
                            pData.ws.send(JSON.stringify(event));
                        }
                    }
                }
                return;
            }

            // XỬ LÝ LỆNH XOÁ TOÀN BỘ LỊCH SỬ 2 CHIỀU (Clear History for Both)
            if (action === 'clear_room_history') {
                if (currentRoom && roomMailboxes.has(currentRoom)) {
                    roomMailboxes.delete(currentRoom);
                    console.log(`[clear_history] Đã tiêu huỷ toàn bộ hộp thư phòng: ${currentRoom}`);
                }
                // Chuyển tiếp lệnh xoá tới máy đối phương
                if (currentRoom && rooms.has(currentRoom)) {
                    const roomParticipants = rooms.get(currentRoom);
                    for (const [pId, pData] of roomParticipants.entries()) {
                        if (pId !== senderId && pData.ws.readyState === WebSocket.OPEN) {
                            pData.ws.send(JSON.stringify(event));
                        }
                    }
                }
                return;
            }

            // XỬ LÝ GỬI TIN NHẮN (action === 'message')
            if (action === 'message') {
                // Lưu vào Hộp thư phòng (E2EE Dead-Drop Mailbox)
                const targetRoom = room || currentRoom;
                if (targetRoom) {
                    if (!roomMailboxes.has(targetRoom)) {
                        roomMailboxes.set(targetRoom, []);
                    }
                    const mailbox = roomMailboxes.get(targetRoom);
                    const msgId = messageId || (Date.now() + '-' + Math.random().toString(36).substr(2, 9));
                    mailbox.push({
                        messageId: msgId,
                        senderId,
                        senderName,
                        payload,
                        timestamp: timestamp || (Date.now() / 1000)
                    });
                    // Giới hạn tối đa 50 tin gần nhất
                    if (mailbox.length > 50) {
                        mailbox.shift();
                    }
                }
            }

            // Chuyển tiếp tin nhắn / sự kiện (message, typing, image) tới máy còn lại nếu đang online
            if (currentRoom && rooms.has(currentRoom)) {
                const roomParticipants = rooms.get(currentRoom);
                for (const [pId, pData] of roomParticipants.entries()) {
                    if (pId !== senderId && pData.ws.readyState === WebSocket.OPEN) {
                        pData.ws.send(JSON.stringify(event));
                    }
                }
            }

            // Nếu người dùng chủ động gửi lệnh "leave" (Bấm nút Close / Hủy phòng)
            if (action === 'leave') {
                handleUserExplicitLeave(currentRoom, currentSenderId, ws);
                currentRoom = null;
                currentSenderId = null;
            }

        } catch (err) {
            console.error('[!] Lỗi xử lý message:', err.message);
        }
    });

    ws.on('close', () => {
        if (currentRoom && currentSenderId) {
            handleSocketDisconnect(currentRoom, currentSenderId, ws);
        }
    });

    // Xử lý khi người dùng CHỦ ĐỘNG bấm Thoát (Close)
    function handleUserExplicitLeave(roomCode, senderId, closingWs) {
        if (!roomCode || !rooms.has(roomCode)) return;
        const roomParticipants = rooms.get(roomCode);
        
        // Kiểm tra đúng socket này đang sở hữu slot không
        if (roomParticipants.get(senderId)?.ws === closingWs) {
            roomParticipants.delete(senderId);
            console.log(`[-] Người dùng (${senderId}) chủ động rời phòng: ${roomCode}. Còn lại: ${roomParticipants.size}`);

            // Báo cho máy còn lại biết đối phương đã rời phòng
            for (const [pId, pData] of roomParticipants.entries()) {
                if (pData.ws.readyState === WebSocket.OPEN) {
                    pData.ws.send(JSON.stringify({
                        action: 'leave',
                        room: roomCode,
                        senderId: senderId,
                        senderName: 'Đối phương',
                        payload: 'Đối phương đã rời phòng',
                        timestamp: Date.now() / 1000
                    }));
                }
            }
        }

        pruneRoom(roomCode);
    }

    // Xử lý khi socket bị ngắt kết nối do mạng (4G drop, tắt app đột ngột, etc.)
    function handleSocketDisconnect(roomCode, senderId, closingWs) {
        if (!roomCode || !rooms.has(roomCode)) return;
        const roomParticipants = rooms.get(roomCode);

        // QUAN TRỌNG: Chỉ xử lý nếu socket đóng chính là socket hiện tại trong phòng
        // Tránh Race Condition: Nếu máy đó đã reconnect trên socket mới, không được xoá socket mới!
        if (roomParticipants.get(senderId)?.ws !== closingWs) {
            console.log(`[ignore] Bỏ qua close event của socket cũ (${senderId}) vì đã có socket mới.`);
            return;
        }

        roomParticipants.delete(senderId);
        console.log(`[drop] Mất kết nối socket của (${senderId}) trong phòng: ${roomCode}. Còn lại: ${roomParticipants.size}`);

        // Thông báo cho máy còn lại rằng đối phương vừa bị ngắt kết nối mạng (đang chờ reconnect)
        for (const [pId, pData] of roomParticipants.entries()) {
            if (pData.ws.readyState === WebSocket.OPEN) {
                pData.ws.send(JSON.stringify({
                    action: 'peer_disconnected',
                    room: roomCode,
                    senderId: senderId,
                    senderName: 'Đối phương',
                    payload: 'Đối phương tạm thời mất kết nối mạng',
                    timestamp: Date.now() / 1000
                }));
            }
        }

        pruneRoom(roomCode);
    }
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 SwiftChat Zero-Storage Relay Server đang chạy tại cổng ${PORT}`);
});
