const path = require('path');
const dotenv = require('dotenv');
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../backend/.env') });

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { connectRedis } = require('./redis');
const { initPublisher, publishToChatRoom, publishCreditsUpdate, publishPrivateMessage } = require('./pubsub/publisher');
const { initSubscriber } = require('./pubsub/subscriber');
const logger = require('./utils/logger');

const app = express();
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
    credentials: true
  },
  transports: ['polling', 'websocket'],
  pingTimeout: 60000,
  pingInterval: 25000
});

app.get('/health', (req, res) => {
  res.json({ service: 'game-service', status: 'ok', timestamp: new Date().toISOString() });
});

const { handleDicebotCommand } = require('./events/dicebotEvents');
const { handleLowcardCommand } = require('./events/lowcardEvents');
const { handleLegendCommand } = require('./events/legendEvents');
const gameEvents = require('./events/gameEvents');
const gameStateManager = require('./services/gameStateManager');
const { handleRestartRefunds } = require('./services/restartRefundService');

const createGameBroadcaster = (io) => {
  return {
    to: (room) => ({
      emit: (event, data) => {
        io.of('/game').to(room).emit(event, data);
        if (event === 'chat:message') {
          const roomId = room.replace('room:', '');
          publishToChatRoom(roomId, data);
        }
        if (event === 'credits:updated') {
          const roomId = room.replace('room:', '');
          publishCreditsUpdate(roomId, data.userId, data.balance);
        }
      }
    }),
    emit: (event, data) => {
      io.of('/game').emit(event, data);
    }
  };
};

const setupGameNamespace = () => {
  const gameNamespace = io.of('/game');

  gameNamespace.on('connection', (socket) => {
    const username = socket.handshake.auth?.username || 'Anonymous';
    const userId = socket.handshake.auth?.userId || 'Unknown';

    if (username === 'Anonymous' || userId === 'Unknown') {
      socket.emit('error', { message: 'Authentication required', code: 'AUTH_REQUIRED' });
      socket.disconnect(true);
      return;
    }

    logger.info('[Game] Client connected', { socketId: socket.id, username, userId });
    socket.join(`user:${userId}`);

    gameEvents(gameNamespace, socket);

    socket.on('game:room:join', (data) => {
      if (data.roomId) {
        socket.join(`room:${data.roomId}`);
        logger.info('[Game] User joined room', { username, roomId: data.roomId });
      }
    });

    socket.on('game:room:leave', (data) => {
      if (data.roomId) {
        socket.leave(`room:${data.roomId}`);
      }
    });

    const handleGameCommand = async (data) => {
      const { roomId, userId, username, message, command } = data;
      const cmd = message || command;
      if (!roomId || !cmd) return;

      const broadcaster = createGameBroadcaster(io);
      const lowerMessage = cmd.toLowerCase().trim();

      const isBotAdminCommand = lowerMessage.startsWith('/bot ') || lowerMessage.startsWith('/add bot ');

      if (isBotAdminCommand) {
        if (lowerMessage.includes('dice')) {
          const handled = await handleDicebotCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          if (handled) return;
        }
        if (lowerMessage.includes('lowcard')) {
          const handled = await handleLowcardCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          if (handled) return;
        }
        if (lowerMessage.includes('flagh')) {
          const handled = await handleLegendCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          if (handled) return;
        }
        if (lowerMessage.includes('stop')) {
          const lowcardHandled = await handleLowcardCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          if (lowcardHandled) return;
          const legendHandled = await handleLegendCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          if (legendHandled) return;
        }
      }

      const activeGameType = await gameStateManager.getActiveGameType(roomId);

      if (lowerMessage === '!d') {
        if (activeGameType === gameStateManager.GAME_TYPES.LOWCARD) {
          await handleLowcardCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
        }
        return;
      }

      if (lowerMessage === '!r' || lowerMessage === '!roll') {
        if (activeGameType === gameStateManager.GAME_TYPES.DICE) {
          await handleDicebotCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
        }
        return;
      }

      if (lowerMessage === '!fg' || lowerMessage.startsWith('!b ') || lowerMessage === '!lock') {
        if (activeGameType === gameStateManager.GAME_TYPES.FLAGBOT) {
          await handleLegendCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
        } else {
          const legendService = require('./services/legendService');
          const flagbotActive = await legendService.isBotActive(roomId);
          if (flagbotActive) {
            await handleLegendCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          }
        }
        return;
      }

      if (lowerMessage.startsWith('!start') || lowerMessage === '!j' || lowerMessage === '!join' || lowerMessage === '!cancel' || lowerMessage === '!n' || lowerMessage === '!stop' || lowerMessage === '!reset' || lowerMessage === '!rezet') {
        if (activeGameType === gameStateManager.GAME_TYPES.DICE) {
          const handled = await handleDicebotCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          if (handled) return;
        } else if (activeGameType === gameStateManager.GAME_TYPES.LOWCARD) {
          const handled = await handleLowcardCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          if (handled) return;
        } else if (activeGameType === gameStateManager.GAME_TYPES.FLAGBOT) {
          const handled = await handleLegendCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
          if (handled) return;
        } else {
          const { getRedisClient } = require('./redis');
          const redis = getRedisClient();
          const dicebotActive = await redis.exists(`dicebot:bot:${roomId}`);
          if (dicebotActive) {
            const handled = await handleDicebotCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
            if (handled) return;
          }
          const lowcardActive = await redis.exists(`lowcard:bot:${roomId}`);
          if (lowcardActive) {
            const handled = await handleLowcardCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
            if (handled) return;
          }
        }
      }

      const dicebotHandled = await handleDicebotCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
      if (dicebotHandled) return;
      const lowcardHandled = await handleLowcardCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
      if (lowcardHandled) return;
      const legendHandled = await handleLegendCommand(broadcaster, socket, { roomId, userId, username, message: cmd });
      if (legendHandled) return;
    };

    socket.on('game:command', handleGameCommand);
    socket.on('game:command:received', handleGameCommand);

    socket.on('ping', () => socket.emit('pong', { timestamp: Date.now() }));
    socket.on('disconnect', (reason) => {
      logger.info('[Game] Client disconnected', { socketId: socket.id, username, reason });
    });
  });

  return gameNamespace;
};

const GAME_PORT = process.env.GAME_PORT || 3001;

const startGameService = async () => {
  try {
    console.log('Starting MigX Game Service...');

    await connectRedis();
    console.log('Redis connected for Game Service');

    await initPublisher();
    console.log('Redis Pub/Sub publisher ready');

    const broadcaster = createGameBroadcaster(io);

    await initSubscriber(async (data) => {
      const { roomId, userId, username, message, socketId } = data;
      const mockSocket = {
        id: socketId || 'pubsub',
        emit: (event, payload) => {
          if (event === 'chat:message') {
            if (payload.type === 'private') {
              publishPrivateMessage(roomId, userId, payload);
            } else {
              publishToChatRoom(roomId, payload);
            }
          }
        }
      };
      const lowerMessage = (message || '').toLowerCase().trim();

      const activeGameType = await gameStateManager.getActiveGameType(roomId);

      if (lowerMessage.startsWith('/bot ') || lowerMessage.startsWith('/add bot ')) {
        if (lowerMessage.includes('dice')) await handleDicebotCommand(broadcaster, mockSocket, { roomId, userId, username, message });
        else if (lowerMessage.includes('lowcard')) await handleLowcardCommand(broadcaster, mockSocket, { roomId, userId, username, message });
        else if (lowerMessage.includes('flagh')) await handleLegendCommand(broadcaster, mockSocket, { roomId, userId, username, message });
        return;
      }

      if (lowerMessage === '!d' && activeGameType === 'lowcard') {
        await handleLowcardCommand(broadcaster, mockSocket, { roomId, userId, username, message });
      } else if ((lowerMessage === '!r' || lowerMessage === '!roll') && activeGameType === 'dice') {
        await handleDicebotCommand(broadcaster, mockSocket, { roomId, userId, username, message });
      } else if (lowerMessage === '!fg' || lowerMessage.startsWith('!b ') || lowerMessage === '!lock') {
        await handleLegendCommand(broadcaster, mockSocket, { roomId, userId, username, message });
      } else {
        const handled = await handleDicebotCommand(broadcaster, mockSocket, { roomId, userId, username, message });
        if (!handled) {
          const lHandled = await handleLowcardCommand(broadcaster, mockSocket, { roomId, userId, username, message });
          if (!lHandled) await handleLegendCommand(broadcaster, mockSocket, { roomId, userId, username, message });
        }
      }
    });
    console.log('Redis Pub/Sub subscriber ready');

    await handleRestartRefunds(broadcaster);
    console.log('Game restart refund check completed');

    setupGameNamespace();

    const { startTimerPoller: startLowcardTimerPoller } = require('./events/lowcardEvents');
    startLowcardTimerPoller(broadcaster);
    console.log('LowCard timer poller started');

    const { startTimerPoller: startDicebotTimerPoller } = require('./events/dicebotEvents');
    startDicebotTimerPoller(broadcaster);
    console.log('DiceBot timer poller started');

    server.listen(GAME_PORT, '0.0.0.0', () => {
      console.log(`Game Service running on 0.0.0.0:${GAME_PORT}`);
      console.log('╔══════════════════════════════════════════╗');
      console.log('║     MigX Game Microservice Running       ║');
      console.log(`║     Port: ${GAME_PORT}                            ║`);
      console.log('║     Games: DiceBot, LowCard, FlagBot     ║');
      console.log('║     PubSub: Redis                        ║');
      console.log('╚══════════════════════════════════════════╝');
    });
  } catch (error) {
    console.error('Failed to start Game Service:', error);
    process.exit(1);
  }
};

startGameService();
