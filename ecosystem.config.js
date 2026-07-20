// ecosystem.config.js — pm2 start ecosystem.config.js
// Run this per VM with that VM's .env already in place (pm2 reads
// process.env at boot, so `pm2 start` after `export $(cat .env)` or via
// a tool like dotenv/pm2's env_file support in your pm2 version).

module.exports = {
  apps: [
    {
      name: 'gun-chat',
      script: './chat-server.js',
      interpreter: 'node',
      node_args: '--experimental-modules',
      instances: 1, // Gun is stateful in-process; do not cluster this
      exec_mode: 'fork',
      autorestart: true,
      max_restarts: 20,
      min_uptime: '10s',
      restart_delay: 3000,
      max_memory_restart: '1G', // chat is radisk:true — this is a real crash guard, not routine cycling
      kill_timeout: 12000, // give shutdown() time to close gracefully before SIGKILL
    },
    {
      name: 'gun-signal',
      script: './signaling-server.js',
      interpreter: 'node',
      node_args: '--experimental-modules',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      max_restarts: 50, // this one is EXPECTED to restart routinely — see restart-signal-relay.sh
      min_uptime: '10s',
      restart_delay: 3000,
      kill_timeout: 12000,
    },
  ],
};
