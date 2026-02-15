const { query } = require('../db/db');

const getUserById = async (userId) => {
  if (!userId) return null;
  try {
    const result = await query('SELECT id, username, role, credits FROM users WHERE id = $1', [userId]);
    return result.rows[0] || null;
  } catch (err) {
    console.error('getUserById error:', err.message);
    return null;
  }
};

const getUserByUsername = async (username) => {
  if (!username) return null;
  try {
    const result = await query('SELECT id, username, role, credits FROM users WHERE LOWER(username) = LOWER($1)', [username]);
    return result.rows[0] || null;
  } catch (err) {
    console.error('getUserByUsername error:', err.message);
    return null;
  }
};

const isAdmin = async (userId) => {
  const user = await getUserById(userId);
  return user && (user.role === 'admin' || user.role === 'super_admin');
};

module.exports = { getUserById, getUserByUsername, isAdmin };
