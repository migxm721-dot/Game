const { Pool } = require('pg');

const isProduction = process.env.NODE_ENV === 'production';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: isProduction 
    ? { rejectUnauthorized: true }
    : { rejectUnauthorized: false }
});

pool.on('error', (err) => {
  console.error('PostgreSQL pool error:', err);
});

const query = async (text, params) => {
  const start = Date.now();
  try {
    const res = await pool.query(text, params);
    const duration = Date.now() - start;
    if (process.env.NODE_ENV === 'development') {
      console.log('Executed query', { text: text.substring(0, 50), duration, rows: res.rowCount });
    }
    return res;
  } catch (error) {
    console.error('DATABASE_ERROR:', error.message, text.substring(0, 100));
    throw error;
  }
};

const getClient = async () => {
  return await pool.connect();
};

const getPool = () => {
  return pool;
};

module.exports = { pool, query, getClient, getPool };
