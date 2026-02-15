const { createClient } = require('redis');
const logger = require('../utils/logger');

let pubClient = null;

const initPublisher = async () => {
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  pubClient = createClient({ url: redisUrl });
  await pubClient.connect();
  logger.info('[GamePub] Redis publisher connected');
  return pubClient;
};

const publishGameMessage = async (channel, data) => {
  if (!pubClient) return;
  try {
    await pubClient.publish(channel, JSON.stringify(data));
  } catch (err) {
    logger.error('[GamePub] Publish error:', err);
  }
};

const publishToChatRoom = async (roomId, messageData) => {
  await publishGameMessage('game:chat:message', { roomId, messageData });
};

const publishCreditsUpdate = async (roomId, userId, balance) => {
  await publishGameMessage('game:credits:update', { roomId, userId, balance });
};

const publishPrivateMessage = async (roomId, userId, messageData) => {
  await publishGameMessage('game:private:message', { roomId, userId, messageData });
};

module.exports = { initPublisher, publishGameMessage, publishToChatRoom, publishCreditsUpdate, publishPrivateMessage };
