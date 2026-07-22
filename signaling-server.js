// =====================================================================
// signaling-server.js — standalone Gun.js CALL SIGNALING server
//
// Deploy one of these per relay node (signal-1, signal-2, signal-3).
// Every node's GUN_SIGNAL_PEERS env must list the OTHER two nodes so
// clients stay connected to all three simultaneously — losing one to
// a scheduled restart never drops the others.
//
// RAM-only by design (radisk: false). Meant to be restarted on a
// staggered schedule to reclaim memory — see restart-signal-relay.sh.
// =====================================================================

import express from 'express';
import http from 'http';
import Gun from 'gun';

// ---------------------------------------------------------------------
// Env / config
// ---------------------------------------------------------------------
const PORT = parseInt(process.env.SIGNAL_PORT || '8766');
const NODE_ID = process.env.NODE_ID || 'signal-1';
const SIGNAL_TTL_MS = parseInt(process.env.SIGNAL_TTL_MS || '90000'); // backstop sweep
const SWEEP_INTERVAL_MS = parseInt(process.env.SWEEP_INTERVAL_MS || '30000');
const MEMORY_RESTART_THRESHOLD_MB = parseInt(process.env.MEMORY_RESTART_THRESHOLD_MB || '512');

// Comma-separated list of the OTHER signaling relay peers (not this one).
const GUN_SIGNAL_PEERS = (process.env.GUN_SIGNAL_PEERS || '')
  .split(',')
  .map((p) => p.trim())
  .filter(Boolean);

const NOTIFICATION_SERVICE_URL = process.env.NOTIFICATION_SERVICE_URL;
if (!NOTIFICATION_SERVICE_URL) {
  console.error('FATAL: NOTIFICATION_SERVICE_URL is not set. Refusing to start.');
  process.exit(1);
}

// ---------------------------------------------------------------------
// App / HTTP server
// ---------------------------------------------------------------------
const app = express();
const server = http.createServer(app);

// CRITICAL: must be registered before any other routes. See chat-server.js
// for why — Express 404s /gun before Gun handles it without this.
app.use(Gun.serve);

app.get('/health', (req, res) => {
  const mem = process.memoryUsage();
  res.status(200).json({
    status: 'ok',
    node: NODE_ID,
    peers: GUN_SIGNAL_PEERS,
    activeCalls: callTimestamps.size,
    memoryMB: Math.round(mem.rss / 1024 / 1024),
    memoryRestartThresholdMB: MEMORY_RESTART_THRESHOLD_MB,
    uptimeSeconds: Math.floor(process.uptime()),
    timestamp: Date.now(),
  });
});

// Lets restart-signal-relay.sh decide "should I restart this node" without
// needing to shell into the box and read /proc — pure signal, no action.
app.get('/should-restart', (req, res) => {
  const mem = process.memoryUsage();
  const memoryMB = Math.round(mem.rss / 1024 / 1024);
  res.status(200).json({ shouldRestart: memoryMB > MEMORY_RESTART_THRESHOLD_MB, memoryMB });
});

// ---------------------------------------------------------------------
// Gun init — RAM only
// ---------------------------------------------------------------------
let signalingGun = null;

function initializeSignalingGun() {
  signalingGun = Gun({
    web: server,
    peers: GUN_SIGNAL_PEERS,
    radisk: false,
    localStorage: false,
    file: false,
    // See chat-server.js — AXE routing behavior instead of store-and-serve
    // caused synced edges pointing at unretrievable nodes.
    axe: false,
    multicast: false,
  });

  signalingGun.on('hi', (peer) => console.log(`[Gun] Peer connected: ${peer?.url || peer?.id || 'unknown'}`));
  signalingGun.on('bye', (peer) => console.log(`[Gun] Peer disconnected: ${peer?.url || peer?.id || 'unknown'}`));

  return signalingGun;
}

// ---------------------------------------------------------------------
// Write/consume helpers (also usable if you want server-side signaling
// helpers/tests — clients normally write directly via their own Gun
// instance pointed at these same peers, per GunCallSignaling.js)
// ---------------------------------------------------------------------
const callTimestamps = new Map(); // callId -> last-write time, drives the sweep

function touchCall(callId) {
  callTimestamps.set(callId, Date.now());
}

function consumeAndClear(callId) {
  if (!signalingGun) return;
  signalingGun.get('calls').get(callId).put(null);
  callTimestamps.delete(callId);
}

// Track writes for the sweep even though clients write directly — listen
// at the relay level so we catch calls regardless of which client wrote.
function attachSweepTracking() {
  signalingGun.get('calls').map().on((data, callId) => {
    if (!callId || callId === '_') return;
    touchCall(callId);
  });
}

// ---------------------------------------------------------------------
// Backstop sweep — clears any call signaling entry older than
// SIGNAL_TTL_MS. Safety net for calls that drop before the client's own
// tombstone-on-hangup fires.
// ---------------------------------------------------------------------
let sweepInterval = null;

function startSweep() {
  if (sweepInterval) clearInterval(sweepInterval);
  sweepInterval = setInterval(() => {
    const now = Date.now();
    for (const [callId, ts] of callTimestamps.entries()) {
      if (now - ts > SIGNAL_TTL_MS) {
        consumeAndClear(callId);
      }
    }
  }, SWEEP_INTERVAL_MS);
}

function stopSweep() {
  if (sweepInterval) {
    clearInterval(sweepInterval);
    sweepInterval = null;
  }
}

// ---------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------
initializeSignalingGun();
attachSweepTracking();
startSweep();

server.listen(PORT, () => {
  console.log(`[signal-server:${NODE_ID}] Listening on :${PORT}`);
  console.log(`[signal-server:${NODE_ID}] Peers: ${GUN_SIGNAL_PEERS.join(', ') || '(none configured)'}`);
  console.log(`[signal-server:${NODE_ID}] Notification service: ${NOTIFICATION_SERVICE_URL}`);
});

// ---------------------------------------------------------------------
// Graceful shutdown — SIGTERM only. pm2/systemd is responsible for
// bringing the process back up; this module never restarts itself.
// ---------------------------------------------------------------------
function shutdown(signal) {
  console.log(`[signal-server:${NODE_ID}] ${signal} received, shutting down gracefully...`);
  stopSweep();
  callTimestamps.clear();
  server.close(() => {
    console.log(`[signal-server:${NODE_ID}] HTTP server closed`);
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 10000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (err) => {
  console.error(`[signal-server:${NODE_ID}] Uncaught exception:`, err);
});
process.on('unhandledRejection', (err) => {
  console.error(`[signal-server:${NODE_ID}] Unhandled rejection:`, err);
});

export { consumeAndClear };