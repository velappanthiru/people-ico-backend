const fp = require('fastify-plugin');
const mysql = require('mysql2');
const path = require('path');
// Load .env from the server directory explicitly so env vars are available
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const mysqlPlugin = async (fastify, options) => {
  const pool = mysql.createPool({
    connectionLimit: 10,
    host: '127.0.0.1',
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    connectTimeout: 10000
  });

  const executeQuery = (sql, params) => {
    return new Promise((resolve, reject) => {
      pool.query(sql, params, (err, results) => {
        if (err) {
          reject(err);
        } else {
          resolve(results);
        }
      });
    });
  };

  fastify.decorate('mysql', {
    query: executeQuery,
    pool: pool
  });
};

module.exports = fp(mysqlPlugin);
