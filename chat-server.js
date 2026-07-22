// =====================================================================
// chat-server.js — standalone Gun.js CHAT + GROUP server
//
// Deploy one of these per relay node. Every node's GUN_CHAT_PEERS env
// must list the OTHER relay nodes (not itself) so the mesh is fully
// connected — see .env.example.
// =====================================================================
import dotenv from 'dotenv';
import express from 'express';
import http from 'http';
import Gun from 'gun';
import { LRUCache } from 'lru-cache';
import Redis from 'ioredis';

// ---------------------------------------------------------------------
// Env / config
// ---------------------------------------------------------------------
const PORT = parseInt(process.env.CHAT_PORT || '8765');
const NODE_ID = process.env.NODE_ID || 'chat-1';
const GUN_TIMEOUT = parseInt(process.env.GUN_TIMEOUT || '9000');
const DEFAULT_GROUP_ID = process.env.DEFAULT_GROUP_ID || 'first_responder_group';

// Shared across all 3 chat relay nodes — this is what makes notification
// dedup actually work. The in-memory processedEvents cache below only
// dedups within ONE process; since the same message arrives on all 3
// relays via Gun sync, only a lock shared across all 3 stops all 3
// from independently sending the same push.
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379');
const REDIS_PASSWORD = process.env.REDIS_PASSWORD || undefined;
const NOTIF_CLAIM_TTL_SEC = parseInt(process.env.NOTIF_CLAIM_TTL_SEC || '120');

const redis = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  password: REDIS_PASSWORD,
  retryStrategy: (times) => Math.min(times * 50, 2000),
  maxRetriesPerRequest: 3,
});
redis.on('error', (err) => console.error(`[Redis] ${err.message}`));

// Atomically claims the right to notify for this messageId. Returns true
// only for whichever of the 3 chat-server processes calls this first;
// the other two get false and skip sending. NX = only set if absent,
// EX = auto-expire so a crash mid-claim doesn't wedge it forever.
async function claimNotification(messageId) {
  try {
    const result = await redis.set(`notif-claim:${messageId}`, NODE_ID, 'EX', NOTIF_CLAIM_TTL_SEC, 'NX');
    return result === 'OK';
  } catch (error) {
    // Redis unreachable — fail open (send anyway) rather than silently
    // drop notifications; a rare duplicate push beats a rare missed one.
    console.error('[Notify] Redis claim failed, sending without dedup:', error.message);
    return true;
  }
}

// Comma-separated list of the OTHER chat relay peers (not this one).
// e.g. on chat-1: "wss://chat-2.trustgrid.com/gun,wss://chat-3.trustgrid.com/gun"
const GUN_CHAT_PEERS = (process.env.GUN_CHAT_PEERS || '')
  .split(',')
  .map((p) => p.trim())
  .filter(Boolean);

// The standalone notification service (index.js / notificationService.js)
// const NOTIFICATION_SERVICE_URL = process.env.NOTIFICATION_SERVICE_URL;
// if (!NOTIFICATION_SERVICE_URL) {
//   console.error('FATAL: NOTIFICATION_SERVICE_URL is not set. Refusing to start.');
//   process.exit(1);
// }

// ---------------------------------------------------------------------
// App / HTTP server — Gun attaches to this via { web }
// ---------------------------------------------------------------------
const app = express();
const server = http.createServer(app);

// CRITICAL: must be registered before any other routes/catch-alls.
// Without this, Express's own 404 handler answers GET/POST /gun before
// Gun's request handling ever runs — which is exactly what breaks both
// client→relay AND relay→relay (peer) traffic.
app.use(Gun.serve);

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    node: NODE_ID,
    peers: GUN_CHAT_PEERS,
    activeChatHandlers: activeEventHandlers.chats.size,
    activeGroupHandlers: activeEventHandlers.groupChats.size,
    uptimeSeconds: Math.floor(process.uptime()),
    memoryMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    timestamp: Date.now(),
  });
});

// ---------------------------------------------------------------------
// State
// ---------------------------------------------------------------------
const appState = { gun: null };

const activeEventHandlers = {
  chats: new Set(),
  groupChats: new Set(),
};

const processedEvents = new LRUCache({ max: 50000, ttl: 600000, updateAgeOnGet: true });
const pushDebounceMap = new Map();

// ---------------------------------------------------------------------
// Notification bridge (HTTP → notificationService.js /api/send-notification)
// ---------------------------------------------------------------------
async function sendCombinedNotification(userAlias, notificationData) {
  try {
    const res = await fetch(`${NOTIFICATION_SERVICE_URL}/api/send-notification`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userAlias, notificationData }),
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) {
      console.error(`[Notify] Service returned ${res.status} for ${userAlias}`);
    }
  } catch (error) {
    console.error(`[Notify] Failed to reach notification service for ${userAlias}:`, error.message);
  }
}

function debouncedCombinedNotification(userAlias, data, delay = 1000) {
  const key = `${userAlias}-${data.type}`;
  if (pushDebounceMap.has(key)) clearTimeout(pushDebounceMap.get(key));

  const timeoutId = setTimeout(async () => {
    try {
      await sendCombinedNotification(userAlias, data);
    } catch (error) {
      console.error('[Notify] Debounced notification failed:', error);
    }
    pushDebounceMap.delete(key);
  }, delay);

  pushDebounceMap.set(key, timeoutId);
}

// ---------------------------------------------------------------------
// Gun init
// ---------------------------------------------------------------------
function initializeChatGun() {
  appState.gun = Gun({
    web: server,
    file: `data-${NODE_ID}`,
    peers: GUN_CHAT_PEERS,
    localStorage: false,
    radisk: true,
    until: GUN_TIMEOUT,
    chunk: 1024 * 8,
    // AXE turns a relay into a routing peer rather than a storing one —
    // it forwards messages between peers instead of reliably persisting
    // and serving them back. That produced edges that synced while the
    // nodes they pointed at were never retrievable. We want plain
    // store-and-serve relay behavior here.
    axe: false,
    // LAN multicast discovery is meaningless for geographically separate
    // VMs and only adds noise; peers are configured explicitly.
    multicast: false,
  });

  appState.gun.on('hi', (peer) => console.log(`[Gun] Peer connected: ${peer?.url || peer?.id || 'unknown'}`));
  appState.gun.on('bye', (peer) => console.log(`[Gun] Peer disconnected: ${peer?.url || peer?.id || 'unknown'}`));

  setupEventHandlers();
  return appState.gun;
}

// ---------------------------------------------------------------------
// Message persistence helper (used for the default/broadcast group)
// ---------------------------------------------------------------------
async function saveMessageToGun(messageData) {
  if (!messageData || typeof messageData !== 'object') {
    throw new Error('Invalid message data: must be a non-null object');
  }
  if (!messageData.sender || !messageData.content || !messageData.timestamp) {
    throw new Error('Invalid message data: missing required fields');
  }

  return new Promise((resolve, reject) => {
    if (!appState.gun) return reject(new Error('Database not initialized'));

    const messageId = `msg_${Date.now()}_${Math.random().toString(16).slice(2, 10)}`;
    const timer = setTimeout(() => reject(new Error('Timeout while saving message to Gun')), GUN_TIMEOUT);

    appState.gun
      .get('groupChats')
      .get(DEFAULT_GROUP_ID)
      .set(messageData, (ack) => {
        clearTimeout(timer);
        if (ack.err) reject(new Error(`Failed to save message: ${ack.err}`));
        else resolve(messageId);
      });
  });
}

// ---------------------------------------------------------------------
// Event handlers — .once() for boot snapshot, .on() for genuinely new
// top-level keys created after boot. See inline note on why both.
// ---------------------------------------------------------------------
function setupEventHandlers() {
  try {
    appState.gun.get('chats').map().once((chatNode, chatId) => registerChatHandler(chatId));
    appState.gun.get('chats').map().on((chatNode, chatId) => registerChatHandler(chatId));

    appState.gun.get('groupChats').map().once((chatNode, groupId) => registerGroupHandler(groupId));
    appState.gun.get('groupChats').map().on((chatNode, groupId) => registerGroupHandler(groupId));
  } catch (error) {
    console.error('[Gun] Error setting up event handlers:', error);
  }
}

function registerChatHandler(chatId) {
  if (!chatId || chatId === '_' || activeEventHandlers.chats.has(chatId)) return;
  activeEventHandlers.chats.add(chatId);

  appState.gun.get('chats').get(chatId).map().on(async (message, messageId) => {
    try {
      await handleChatMessage(chatId, message, messageId);
    } catch (error) {
      console.error('[Gun] Error handling chat message:', error);
    }
  });
}

function registerGroupHandler(groupId) {
  if (!groupId || groupId === '_' || activeEventHandlers.groupChats.has(groupId)) return;
  activeEventHandlers.groupChats.add(groupId);

  appState.gun.get('groupChats').get(groupId).map().on(async (message, messageId) => {
    try {
      await handleGroupMessage(groupId, message, messageId);
    } catch (error) {
      console.error('[Gun] Error handling group message:', error);
    }
  });
}

function clearEventHandlers() {
  activeEventHandlers.chats.forEach((chatId) => appState.gun.get('chats').get(chatId).map().off());
  activeEventHandlers.groupChats.forEach((groupId) => appState.gun.get('groupChats').get(groupId).map().off());
  activeEventHandlers.chats.clear();
  activeEventHandlers.groupChats.clear();
}

// ---------------------------------------------------------------------
// 1:1 chat message → notification
// ---------------------------------------------------------------------
function handleChatMessage(chatId, message, messageId) {
  if (!messageId || processedEvents.has(`msg-${messageId}`)) return;
  processedEvents.set(`msg-${messageId}`, true);

  if (!message || !message.sender || message.notified) return;
  if (message.timestamp && Date.now() - message.timestamp > 120000) return; // stale sync replay

  const [user1, user2] = chatId.split('_');
  const recipient = message.sender === user1 ? user2 : user1;
  if (!recipient) return;

  claimNotification(messageId).then((claimed) => {
    if (!claimed) return; // another chat-server node already sent this one

    // debouncedCombinedNotification(recipient, {
    //   type: 'chat',
    //   title: `Message from ${message.sender}`,
    //   body: message.type === 'file' ? 'Sent a file' : message.content,
    //   data: {
    //     messageType: 'chat',
    //     senderId: message.sender,
    //     chatId,
    //     messageId,
    //     contentType: message.type || 'text',
    //     timestamp: message.timestamp || Date.now(),
    //   },
    // });

    appState.gun.get('chats').get(chatId).get(messageId).get('notified').put(true);
  });
}

// ---------------------------------------------------------------------
// Group chat message → notification (fan-out to members)
// ---------------------------------------------------------------------
function handleGroupMessage(groupId, message, messageId) {
  if (!messageId || processedEvents.has(`group-msg-${messageId}`)) return;
  processedEvents.set(`group-msg-${messageId}`, true);

  if (!message || !message.sender || message.notified) return;

  claimNotification(messageId).then((claimed) => {
    if (!claimed) return; // another chat-server node already ran this fan-out

    appState.gun.get('groupChats').get(groupId).get(messageId).get('notified').put(true);

    appState.gun.get('groups').get(groupId).once((groupData) => {
    if (!groupData) {
      console.warn(`[Gun] No groupData found for groupId: ${groupId}`);
      return;
    }

    const groupName = groupData.name || 'Group Message';
    const contentType = message.type || 'text';
    let messagePreview;

    try {
      if (contentType === 'text') {
        messagePreview =
          message.content && message.content.length > 100
            ? message.content.substring(0, 97) + '...'
            : message.content;
      } else if (contentType === 'file') {
        try {
          const fileInfo = JSON.parse(message.content);
          messagePreview = fileInfo.fileType === 'image' ? 'Sent an image' : `Sent a ${fileInfo.fileType || 'file'}`;
        } catch {
          messagePreview = 'Sent a file';
        }
      } else if (contentType === 'location') {
        messagePreview = 'Shared a location';
      } else {
        messagePreview = `New ${contentType} message`;
      }
    } catch (error) {
      console.error('[Gun] Error creating message preview:', error);
      messagePreview = 'New message';
    }

    // members is a Gun soul reference — must be read as its own node.
    appState.gun.get('groups').get(groupId).get('members').once((membersData) => {
      if (!membersData) {
        console.warn(`[Gun] No members found for groupId: ${groupId}`);
        return;
      }

      Object.keys(membersData).forEach((memberAlias) => {
        if (memberAlias === '_' || memberAlias === message.sender) return;

        debouncedCombinedNotification(memberAlias, {
          type: 'group',
          title: groupName,
          body: `${message.sender}: ${messagePreview}`,
          data: {
            messageType: 'group',
            senderId: message.sender,
            groupId,
            messageId,
            contentType,
            timestamp: message.timestamp || Date.now(),
            badge: '1',
            sound: 'default',
          },
        });
      });
    });
  });
  });
}

// ---------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------
initializeChatGun();

server.listen(PORT, () => {
  console.log(`[chat-server:${NODE_ID}] Listening on :${PORT}`);
  console.log(`[chat-server:${NODE_ID}] Peers: ${GUN_CHAT_PEERS.join(', ') || '(none configured)'}`);
  console.log(`[chat-server:${NODE_ID}] Notification service: ${NOTIFICATION_SERVICE_URL}`);
});

// ---------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------
function shutdown(signal) {
  console.log(`[chat-server:${NODE_ID}] ${signal} received, shutting down gracefully...`);
  clearEventHandlers();
  server.close(() => {
    console.log(`[chat-server:${NODE_ID}] HTTP server closed`);
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 10000).unref(); // hard exit if close hangs
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (err) => {
  console.error(`[chat-server:${NODE_ID}] Uncaught exception:`, err);
});
process.on('unhandledRejection', (err) => {
  console.error(`[chat-server:${NODE_ID}] Unhandled rejection:`, err);
});

export { appState, saveMessageToGun };