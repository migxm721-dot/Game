const checkTransferLimit = async (userId) => ({ allowed: true });
const checkFlood = async (username) => ({ allowed: true });
const checkGlobalRateLimit = async (userId) => ({ allowed: true });
const checkGameLimit = async (userId) => ({ allowed: true });
module.exports = { checkTransferLimit, checkFlood, checkGlobalRateLimit, checkGameLimit };
