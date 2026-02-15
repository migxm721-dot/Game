const { createClient } = require('redis');
const logger = require('../utils/logger');

let subClient = null;
let commandHandler = null;

const roomQueues = new Map();

const processRoomQueue = async (roomId) => {
  const queue = roomQueues.get(roomId);
  if (!queue || queue.processing || queue.items.length === 0) return;

  queue.processing = true;

  while (queue.items.length > 0) {
    const data = queue.items.shift();
    try {
      await commandHandler(data);
    } catch (err) {
      logger.error(`[GameSub] Command handler error for room ${roomId}:`, err);
    }
  }

  queue.processing = false;
  roomQueues.delete(roomId);
};

const enqueueCommand = (data) => {
  const roomId = data.roomId || 'global';

  if (!roomQueues.has(roomId)) {
    roomQueues.set(roomId, { items: [], processing: false });
  }

  roomQueues.get(roomId).items.push(data);
  processRoomQueue(roomId);
};

const initSubscriber = async (handler) => {
  commandHandler = handler;
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  subClient = createClient({ url: redisUrl });
  await subClient.connect();
  
  await subClient.subscribe('game:command', (message) => {
    try {
      const data = JSON.parse(message);
      if (commandHandler) {
        enqueueCommand(data);
      }
    } catch (err) {
      logger.error('[GameSub] Parse error:', err);
    }
  });
  
  logger.info('[GameSub] Redis subscriber connected, listening on game:command (with per-room queue)');
  return subClient;
};

module.exports = { initSubscriber };
