const jwt = require('@fastify/jwt');
const path = require('path');
const fs = require('fs');


module.exports = async function (fastify, opts) {
 const { toUtcISOString } = require('./timeUtils');
 fastify.register(jwt, { secret: '()@%)!&#!)' });
 fastify.register(require('@fastify/multipart'), {
 limits: { fileSize: 10 * 1024 * 1024 } // 5 MB limit
 });
 fastify.post('/login', {
 schema: {
 body: {
 type: 'object',
 required: ['email', 'password'],
 properties: {
 email: { type: 'string', format: 'email' },
 password: { type: 'string' }
 }
 }
 }
 }, async (req, reply) => {
 try {
 const { email, password } = req.body;
 console.log(req.body, 'body')
 if (email !== 'admin@ptc.com') {
 return reply.send({ status: false, msg: 'Invalid Email Provided' });
 }

 if (password !== 'Demo@123') {
 return reply.send({ status: false, msg: 'Incorrect Password Provided' });
 }

 const token = fastify.jwt.sign({ email }, { expiresIn: '30m' });

 return reply.send({
 status: true,
 data: {
 email,
 name: 'ADMIN',
 token,
 }
 });
 } catch (err) {
 console.log(err, 'err')
 }
 });
 fastify.get('/dashboard', async (request, reply) => {
 try {
 // Update sale statuses first
 await updateSaleStatuses(fastify);

 // Stats query
 const statsRows = await fastify.mysql.query(`
 SELECT
 (SELECT COUNT(*) FROM users) AS total_users,
 (SELECT COUNT(*) FROM ico_purchases) AS total_transactions,
 (SELECT COUNT(*) FROM ico_purchases WHERE status = 'success') AS successful_transactions,
 (SELECT COUNT(*) FROM ico_purchases WHERE status = 'failed') AS failed_transactions,
 (SELECT COALESCE(SUM(ptc_tokens), 0)
 FROM ico_purchases
 WHERE status = 'success') AS purchased_tokens;`);
 const counts = statsRows[0];

 // Get all sales and find the active one (merge getActiveSales logic)
 // Fetch only the currently active sale (do NOT return upcoming/scheduled sales)
 const activeSaleRows = await fastify.mysql.query(`
 SELECT *,
 CASE
 WHEN NOW() BETWEEN start_at AND end_at THEN 'active'
 WHEN NOW() < start_at THEN 'scheduled'
 ELSE 'ended'
 END AS computed_status
 FROM token_sales
 WHERE status = 'active' OR (NOW() BETWEEN start_at AND end_at)
 ORDER BY
 CASE WHEN status = 'active' THEN 0 ELSE 1 END,
 start_at DESC
 LIMIT 1;
 `);

 let activeSale = Array.isArray(activeSaleRows) ? (activeSaleRows[0] || null) : (activeSaleRows || null);

 // Enrich activeSale with sold and available tokens
 if (activeSale) {
 try {
 const soldRows = await fastify.mysql.query(
 `SELECT COALESCE(SUM(CAST(ptc_tokens AS DECIMAL(30,8))), 0) AS total_tokens_sold
 FROM ico_purchases
 WHERE status = 'success' AND created_at BETWEEN ? AND ?`,
 [activeSale.start_at, activeSale.end_at]
 );
 const sold = parseFloat(soldRows && soldRows[0] ? soldRows[0].total_tokens_sold : 0) || 0;
 const totalQty = Number(activeSale.token_quantity || 0);
 activeSale.total_tokens_sold = sold;
 activeSale.available_tokens = Math.max(0, totalQty - sold);
 } catch (e) {
 fastify.log && fastify.log.warn && fastify.log.warn('Error calculating dashboard activeSale totals:', e.message || e);
 activeSale.total_tokens_sold = 0;
 activeSale.available_tokens = Number(activeSale.token_quantity || 0);
 }
 }

 // Get last 5 transactions
 const lastTxRows = await fastify.mysql.query(`
 SELECT id, address, ptc_tokens, created_at, sale_type, payment_type, usd_value_of_crypto, trans_hash
 FROM ico_purchases
 ORDER BY created_at DESC
 LIMIT 5;
 `);

 return reply.send({
 status: true,
 stats: counts,
 activeSale: activeSale,
 lastTransactions: lastTxRows
 });

 } catch (err) {
 console.error('Error in /dashboard:', err);
 return reply.code(500).send({ status: false, msg: 'Internal Server Error' });
 }
 });

 fastify.get('/users-list', async (request, reply) => {
 try {
 const rows = await fastify.mysql.query(`
 SELECT
 u.wallet_address,
 COALESCE(SUM(ip.ptc_tokens), 0) AS ptc_tokens_purchased,
 COALESCE(SUM(ip.usd_value_of_crypto), 0) AS total_usd_invested
 FROM users u
 LEFT JOIN ico_purchases ip
 ON u.wallet_address = ip.address
 AND LOWER(ip.status) = 'success'
 GROUP BY u.wallet_address
 ORDER BY total_usd_invested DESC;
 `);

 return reply.code(200).send({
 status: true,
 users: rows
 });
 } catch (err) {
 console.error('Error in /users-list:', err);
 return reply.code(500).send({
 status: false,
 msg: 'Internal Server Error'
 });
 }
 });

 // Helper function to update sale statuses based on timestamps
 const updateSaleStatuses = async (fastify) => {
 try {
 await fastify.mysql.query(`
 UPDATE token_sales
 SET status = CASE
 WHEN NOW() BETWEEN start_at AND end_at THEN 'active'
 WHEN NOW() < start_at THEN 'scheduled'
 ELSE 'ended'
 END
 `);
 } catch (err) {
 console.error('Error updating sale statuses:', err);
 }
 };

 fastify.get('/getActiveSales', async (req, res) => {
  try {

    // Always update sale statuses first
    await updateSaleStatuses(fastify);

    // Single clean query for ACTIVE or UPCOMING sale
    const [rows] = await fastify.mysql.query(`
      SELECT *,
      CASE
        WHEN NOW() BETWEEN start_at AND end_at THEN 'active'
        WHEN NOW() < start_at THEN 'scheduled'
        ELSE 'completed'
      END AS computed_status
      FROM token_sales
      WHERE NOW() <= end_at   -- Only active or upcoming
      ORDER BY
        CASE
          WHEN NOW() BETWEEN start_at AND end_at THEN 0  -- Active first
          WHEN NOW() < start_at THEN 1                   -- Upcoming next
          ELSE 2                                         -- Completed (ignored)
        END,
        start_at ASC
      LIMIT 1;
    `);

    const sale = rows?.[0] || null;

    if (!sale) {
      return res.code(200).send({ status: true, sale: null });
    }

    // Fetch sold tokens only within sale window
    try {
      const [soldRows] = await fastify.mysql.query(
        `SELECT COALESCE(SUM(CAST(ptc_tokens AS DECIMAL(30,8))), 0) AS total_tokens_sold
         FROM ico_purchases
         WHERE status = 'success' AND created_at BETWEEN ? AND ?`,
        [sale.start_at, sale.end_at]
      );

      const sold = parseFloat(soldRows?.total_tokens_sold || 0);
      const totalQty = Number(sale.token_quantity || 0);

      sale.total_tokens_sold = sold;
      sale.available_tokens = Math.max(0, totalQty - sold);

    } catch (e) {
      fastify.log?.warn?.('Error calculating sale totals:', e.message);
      sale.total_tokens_sold = 0;
      sale.available_tokens = Number(sale.token_quantity || 0);
    }

    return res.code(200).send({ status: true, sale });

  } catch (err) {
    console.error('Error in /getActiveSales:', err);
    return res.code(500).send({ status: false, msg: 'Internal Server Error' });
  }
});


 fastify.get('/getAllActiveSales', async (req, res) => {
 try {
 await updateSaleStatuses(fastify);

 const onlyActive = req.query && String(req.query.onlyActive).toLowerCase() === 'true';

 if (onlyActive) {
 const activeRows = await fastify.mysql.query(`
 SELECT *,
 CASE
 WHEN NOW() BETWEEN start_at AND end_at THEN 'active'
 WHEN NOW() < start_at THEN 'scheduled'
 ELSE 'ended'
 END AS computed_status
 FROM token_sales
 WHERE status = 'active' OR (NOW() BETWEEN start_at AND end_at)
 ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END, start_at DESC
 LIMIT 1
 `);

 const active = Array.isArray(activeRows) ? (activeRows[0] || null) : (activeRows || null);
 return res.code(200).send({ status: true, onlyActive: true, sale: active });
 }

 const results = await fastify.mysql.query(`
 SELECT *,
 CASE
 WHEN NOW() BETWEEN start_at AND end_at THEN 'active'
 WHEN NOW() < start_at THEN 'scheduled'
 ELSE 'ended'
 END AS computed_status
 FROM token_sales
 ORDER BY start_at
 `);

 const normalizedSales = Array.isArray(results)
 ? results.map((sale) => ({ ...sale }))
 : results
 ? [{ ...results }]
 : [];

 // Enrich each sale with sold and available tokens (use sale time window)
 const salesWithTotals = await Promise.all(normalizedSales.map(async (sale) => {
 try {
 const soldRows = await fastify.mysql.query(
 `SELECT COALESCE(SUM(CAST(ptc_tokens AS DECIMAL(30,8))), 0) AS total_tokens_sold
 FROM ico_purchases
 WHERE status = 'success' AND created_at BETWEEN ? AND ?`,
 [sale.start_at, sale.end_at]
 );
 const sold = parseFloat(soldRows && soldRows[0] ? soldRows[0].total_tokens_sold : 0) || 0;
 const totalQty = Number(sale.token_quantity || 0);
 const available = Math.max(0, totalQty - sold);
 return { ...sale, total_tokens_sold: sold, available_tokens: available };
 } catch (e) {
 fastify.log && fastify.log.warn && fastify.log.warn('Error calculating sale totals:', e.message || e);
 return { ...sale, total_tokens_sold: 0, available_tokens: Number(sale.token_quantity || 0) };
 }
 }));

 return res.code(200).send({ status: true, sales: salesWithTotals });
 } catch (err) {
 console.error('Error in /getActiveSales:', err);
 return res.code(500).send({ status: false, msg: 'Internal Server Error' });
 }
 });

 fastify.post("/saveNewSale", {
 schema: {
 body: {
 type: "object",
 required: ['type', 'name', 'quantity', 'minimum', 'maximum', 'start_at', 'end_at'],
 properties: {
 type: { type: 'string' },
 name: { type: 'string' },
 quantity: { type: 'number' },
 minimum: { type: 'number' },
 maximum: { type: 'number' },
 start_at: { type: 'string' },
 end_at: { type: 'string' },
 price: { type: ['number', 'null'] }
 }
 }
 }
 }, async (req, res) => {
 try {
 const { type, name, quantity, minimum, maximum, start_at, end_at, price } = req.body;

 if (!type || !name || !quantity || !minimum || !maximum || !start_at || !end_at) {
 return res.send({ status: false, msg: "All fields are required" });
 }
 // Enforce maximum number of upcoming/active sales (limit = 5)
 try {
 const activeOrScheduledCountRows = await fastify.mysql.query(`
 SELECT COUNT(*) AS cnt FROM token_sales
 WHERE (
 status = 'active' OR status = 'scheduled' OR (NOW() BETWEEN start_at AND end_at)
 )
 `);
 const currentCount = activeOrScheduledCountRows && activeOrScheduledCountRows[0] ? parseInt(activeOrScheduledCountRows[0].cnt, 10) : 0;
 const MAX_SALES = 5;
 if (currentCount >= MAX_SALES) {
 return res.send({ status: false, msg: `Cannot create new sale: maximum of ${MAX_SALES} upcoming/active sales reached.` });
 }
 } catch (countErr) {
 fastify.log.error('Error checking existing sales count:', countErr);
 // continue - do not block creation on count query failure, but log error
 }

 // Check for overlapping sales with comprehensive validation and configurable buffer (in minutes)
 const bufferMinutes = 1; // Configurable buffer in minutes (can be changed to any value)
 const overlappingSales = await fastify.mysql.query(`
 SELECT id, name, start_at, end_at, status,
 CASE
 WHEN NOW() BETWEEN start_at AND end_at THEN 'active'
 WHEN NOW() < start_at THEN 'scheduled'
 ELSE 'ended'
 END AS computed_status
 FROM token_sales
 WHERE (
 (start_at <= ? AND end_at >= ?) OR
 (start_at <= ? AND end_at >= ?) OR
 (start_at >= ? AND end_at <= ?) OR
 (? <= DATE_ADD(end_at, INTERVAL ? MINUTE) AND ? >= end_at)
 )
 AND (
 (end_at >= NOW()) OR
 (NOW() BETWEEN start_at AND end_at)
 )
 ORDER BY start_at
 `, [start_at, start_at, end_at, end_at, start_at, end_at, start_at, bufferMinutes, start_at]);

 if (overlappingSales.length > 0) {
 const conflictingSale = overlappingSales[0];
 const bufferMessage = conflictingSale.end_at && start_at <= new Date(new Date(conflictingSale.end_at).getTime() + bufferMinutes * 60 * 1000).toISOString().slice(0, 19).replace('T', ' ')
 ? ` and violates ${bufferMinutes}-minute buffer requirement`
 : '';

 return res.send({
 status: false,
 msg: `Sale dates overlap with existing "${conflictingSale.name}" sale (${conflictingSale.start_at} to ${conflictingSale.end_at})${bufferMessage}. New sales must start at least ${bufferMinutes} minutes after previous sale ends.`
 });
 }

 const result = await fastify.mysql.query(
 `INSERT INTO token_sales (type, name, token_quantity, price, minimum_purchase, maximum_purchase, start_at, end_at, status)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?,
 CASE WHEN ? <= NOW() AND ? >= NOW() THEN 'active' ELSE 'scheduled' END)`,
 [type, name, quantity, price, minimum, maximum, start_at, end_at, start_at, end_at]
 );

 // Update statuses after creating new sale
 await updateSaleStatuses(fastify);

 res.code(200).send({ status: true, msg: "Sale created successfully" });
 } catch (err) {
 console.error(err);
 return res.code(500).send({ status: false, msg: 'Internal Server Error' });
 }
 });


 // Get sale details by id including tokens sold within its start/end window
 fastify.get('/getSaledata/:id', async (req, res) => {
 try {
 const id = parseInt(req.params.id, 10);
 if (!id) {
 return res.code(400).send({ status: false, msg: 'Invalid sale id' });
 }

 const rows = await fastify.mysql.query(
 `SELECT id, type, name, token_quantity, price, start_at, end_at, status
 FROM token_sales
 WHERE id = ? LIMIT 1`,
 [id]
 );

 if (!rows || rows.length === 0) {
 return res.code(404).send({ status: false, msg: 'Sale not found' });
 }

 const sale = rows[0];

 const soldRows = await fastify.mysql.query(
 `SELECT COALESCE(SUM(CAST(ptc_tokens AS DECIMAL(30,8))), 0) AS total_tokens_sold
 FROM ico_purchases
 WHERE LOWER(TRIM(status)) = 'success'
 AND created_at BETWEEN ? AND ?`,
 [sale.start_at, sale.end_at]
 );

 const total_tokens_sold = parseFloat(soldRows?.[0]?.total_tokens_sold || 0);

 const responseSale = {
 id: sale.id,
 type: sale.type,
 name: sale.name,
 quantity: sale.token_quantity,
 price: sale.price,
 start_at: sale.start_at,
 end_at: sale.end_at,
 status: sale.status,
 total_tokens_sold: total_tokens_sold
 };

 return res.code(200).send({ status: true, sale: responseSale });
 } catch (err) {
 fastify.log && fastify.log.error && fastify.log.error('Error in /getSale/:id', err);
 return res.code(500).send({ status: false, msg: 'Internal Server Error' });
 }
 });



 fastify.get('/getTransactionDetails', async (request, reply) => {
 try {
 const results = await fastify.mysql.query(`
 SELECT * from ico_purchases
 `);
 const normalized = Array.isArray(results) ? results.map((r) => ({ ...r, created_at_utc: toUtcISOString(r.created_at) })) : results;
 return reply.code(200).send({
 status: true,
 userData: normalized
 });
 } catch (err) {
 console.error(err);
 return reply.code(500).send({ status: false, msg: 'Internal Server Error' });
 }
 });

 fastify.get('/getStakeDetails', async (request, reply) => {
 try {
 // Mark stakes as MATURE when valid_till has passed and they are not withdrawn
 try {
 await fastify.mysql.query(
 `UPDATE stakes SET withdraw_status = 'MATURE'
 WHERE valid_till <= NOW() AND withdraw_status NOT IN ('SUCCESS','MATURE')`
 );
 } catch (uErr) {
 fastify.log.warn('Unable to mark matured stakes:', uErr.message || uErr);
 }

 const results = await fastify.mysql.query(`
 SELECT * from stakes
 `);
 const normalized = Array.isArray(results) ? results.map((r) => ({ ...r, created_at_utc: toUtcISOString(r.created_at), valid_till_utc: toUtcISOString(r.valid_till) })) : results;
 return reply.code(200).send({
 status: true,
 userData: normalized
 });
 } catch (err) {
 console.error(err);
 return reply.code(500).send({ status: false, msg: 'Internal Server Error' });
 }
 });

 // Sync a stake's withdraw event from-chain and persist tx hash into DB
 fastify.post('/syncStakeTx', async (request, reply) => {
 const { stake_id } = request.body || {};
 if (!stake_id) return reply.code(400).send({ success: false, message: 'stake_id is required' });

 try {
 const rows = await fastify.mysql.query('SELECT * FROM stakes WHERE id = ?', [stake_id]);
 if (!rows || rows.length === 0) return reply.code(404).send({ success: false, message: 'stake not found' });
 const stake = rows[0];

 // Resolve client Configs.js dynamically (ES module) to reuse ABI/address
 const { pathToFileURL } = require('url');
 const cfgPath = path.resolve(__dirname, '../../src/Utils/Configs.js');
 let ICO = null;
 try {
 const mod = await import(pathToFileURL(cfgPath).href);
 ICO = mod?.ICO || null;
 } catch (impErr) {
 fastify.log.error('Failed to import Configs.js for ABI/address', impErr);
 }

 if (!ICO || !ICO.STAKING_CONTRACT || !ICO.STAKING_ABI) {
 return reply.code(500).send({ success: false, message: 'Staking contract info not available' });
 }

 const ethers = require('ethers');
 const providerUrl = process.env.RPC_URL || process.env.RPC || null;
 if (!providerUrl) {
 return reply.code(500).send({ success: false, message: 'RPC_URL not configured on server' });
 }

 const provider = new ethers.providers.JsonRpcProvider(providerUrl);
 const contract = new ethers.Contract(ICO.STAKING_CONTRACT, ICO.STAKING_ABI, provider);

 const userAddr = stake.user_address || stake.address || stake.wallet_address || null;
 if (!userAddr) return reply.code(400).send({ success: false, message: 'No user address on stake record' });

 const latestBlock = await provider.getBlockNumber();
 const fromBlock = Math.max(0, latestBlock - 200000); // search last ~200k blocks (configurable)

 const filter = contract.filters.Withdraw(userAddr);
 const events = await contract.queryFilter(filter, fromBlock, latestBlock);

 if (!events || events.length === 0) return reply.code(404).send({ success: false, message: 'No withdraw events found for this user in recent blocks' });

 // Prefer an event after valid_till if available, otherwise latest
 let matched = null;
 if (stake.valid_till) {
 const validT = Math.floor(new Date(stake.valid_till).getTime() / 1000);
 for (let i = events.length - 1; i >= 0; i--) {
 const ev = events[i];
 try {
 const blk = await provider.getBlock(ev.blockNumber);
 if (blk && blk.timestamp && blk.timestamp >= validT) {
 matched = ev;
 break;
 }
 } catch (e) {
 // ignore
 }
 }
 }

 if (!matched) matched = events[events.length - 1];
 const txHash = matched.transactionHash;
 if (!txHash) return reply.code(500).send({ success: false, message: 'Found event has no txHash' });

 await fastify.mysql.query('UPDATE stakes SET withdraw_hash = ?, withdraw_status = ? WHERE id = ?', [txHash, 'SUCCESS', stake_id]);
 const updated = await fastify.mysql.query('SELECT * FROM stakes WHERE id = ?', [stake_id]);
 return reply.send({ success: true, data: updated[0] });
 } catch (err) {
 fastify.log.error(err);
 return reply.code(500).send({ success: false, message: 'Internal error', error: err.message });
 }
 });

 fastify.get('/getreferralClaimDetails', async (request, reply) => {
 try {
 const results = await fastify.mysql.query(`
 SELECT * from referral_claims
 `);
 return reply.code(200).send({
 status: true,
 userData: results
 });
 } catch (err) {
 console.error(err);
 return reply.code(500).send({ status: false, msg: 'Internal Server Error' });
 }
 });

 fastify.get("/settings", async (request, reply) => {
 const rows = await fastify.mysql.query("SELECT * FROM settings LIMIT 1");
 if (rows.length === 0) {
 return reply.send({ status: false, msg: "No settings found" });
 }
 return reply.send({ status: true, data: rows[0] });
 });
 fastify.post("/updatesettings", async (request, reply) => {
 try {
 const data = request.body || {};

 const saveFileFromBase64 = (base64DataUrl, destDir, filenamePrefix) => {
 if (!base64DataUrl || typeof base64DataUrl !== "string") return null;
 const matches = base64DataUrl.match(/^data:([A-Za-z-+/]+);base64,(.+)$/);
 if (!matches) return null;
 const mime = matches[1]; // e.g. image/png
 const b64 = matches[2];
 const ext = mime.split("/")[1] || "bin";
 const filename = `${filenamePrefix}_${Date.now()}.${ext}`;
 const absDir = path.resolve(__dirname, destDir);
 fs.mkdirSync(absDir, { recursive: true });
 const absPath = path.join(absDir, filename);
 fs.writeFileSync(absPath, Buffer.from(b64, "base64"));
 return `/uploads/${filename}`;
 };

 const rows = await fastify.mysql.query("SELECT * FROM settings LIMIT 1");
 const existing = Array.isArray(rows) && rows.length ? rows[0] : null;


 let siteLogoPath;
 if (typeof data.site_logo === "string" && data.site_logo.startsWith("data:")) {
 siteLogoPath = saveFileFromBase64(data.site_logo, "../../public/uploads", "site_logo");
 } else if (typeof data.site_logo === "string" && data.site_logo.length > 0) {
 siteLogoPath = data.site_logo;
 } else {
 siteLogoPath = existing ? existing.site_logo : null;
 }

 let whitepaperPath;
 if (typeof data.whitepaper === "string" && data.whitepaper.startsWith("data:")) {
 whitepaperPath = saveFileFromBase64(data.whitepaper, "../../public/uploads", "whitepaper");
 } else if (typeof data.whitepaper === "string" && data.whitepaper.length > 0) {
 whitepaperPath = data.whitepaper;
 } else {
 whitepaperPath = existing ? existing.whitepaper : null;
 }

 let tokenLogoPath;
 if (typeof data.token_logo === "string" && data.token_logo.startsWith("data:")) {
 tokenLogoPath = saveFileFromBase64(data.token_logo, "../../public/uploads", "token_logo");
 } else if (typeof data.token_logo === "string" && data.token_logo.length > 0) {
 tokenLogoPath = data.token_logo;
 } else {
 tokenLogoPath = existing ? existing.token_logo : null;
 }

 const site_name = data.site_name ?? (existing ? existing.site_name : "");
 const owner_address = data.owner_address ?? (existing ? existing.owner_address : "");
 const referral_level1 = data.referral_level1 ?? (existing ? existing.referral_level1 : 0);
 const referral_level2 = data.referral_level2 ?? (existing ? existing.referral_level2 : 0);
 const referral_level3 = data.referral_level3 ?? (existing ? existing.referral_level3 : 0);
 const kyc_enabled = typeof data.kyc_enabled !== "undefined" ? data.kyc_enabled : (existing ? existing.kyc_enabled : 0);
 const token_name = data.token_name ?? (existing ? existing.token_name : "");
 const token_symbol = data.token_symbol ?? (existing ? existing.token_symbol : "");
 const chain = data.chain ?? (existing ? existing.chain : "");
 const token_decimal = data.token_decimal ?? (existing ? existing.token_decimal : 18);
 const contract_address = data.contract_address ?? (existing ? existing.contract_address : "");
 const crypto_decimal = data.crypto_decimal ?? (existing ? existing.crypto_decimal : 4);
 const fiat_decimal = data.fiat_decimal ?? (existing ? existing.fiat_decimal : 2);

 if (existing) {
 // update existing row (overwrite every column with the computed values)
 await fastify.mysql.query(
 `UPDATE settings SET
 site_name = ?, site_logo = ?, whitepaper = ?, owner_address = ?,
 referral_level1 = ?, referral_level2 = ?, referral_level3 = ?,
 kyc_enabled = ?, token_name = ?, token_symbol = ?, chain = ?, token_decimal = ?,
 contract_address = ?, crypto_decimal = ?, fiat_decimal = ?, token_logo = ?,
 updated_at = CURRENT_TIMESTAMP
 WHERE id = ?`,
 [
 site_name,
 siteLogoPath,
 whitepaperPath,
 owner_address,
 referral_level1,
 referral_level2,
 referral_level3,
 kyc_enabled,
 token_name,
 token_symbol,
 chain,
 token_decimal,
 contract_address,
 crypto_decimal,
 fiat_decimal,
 tokenLogoPath,
 existing.id,
 ]
 );
 } else {
 // insert first row
 await fastify.mysql.query(
 `INSERT INTO settings
 (site_name, site_logo, whitepaper, owner_address, referral_level1, referral_level2, referral_level3,
 kyc_enabled, token_name, token_symbol, chain, token_decimal, contract_address,
 crypto_decimal, fiat_decimal, token_logo)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
 [
 site_name,
 siteLogoPath,
 whitepaperPath,
 owner_address,
 referral_level1,
 referral_level2,
 referral_level3,
 kyc_enabled,
 token_name,
 token_symbol,
 chain,
 token_decimal,
 contract_address,
 crypto_decimal,
 fiat_decimal,
 tokenLogoPath,
 ]
 );
 }

 return reply.send({ status: true, msg: "Settings updated" });
 } catch (err) {
 fastify.log.error("updatesettings error:", err);
 return reply.code(500).send({ status: false, msg: "Error updating settings" });
 }
 });
 fastify.get("/deleteSale/:id", async (req, reply) => {
 const { id } = req.params;
 try {
 await fastify.mysql.query("DELETE FROM token_sales WHERE id = ?", [id]);
 return { status: true, msg: "Sale deleted successfully" };
 } catch (err) {
 reply.code(500).send({ success: false, message: "Failed to delete sale" });
 }
 });
 fastify.route({
 method: "GET",
 url: "/updateTokenPrice",
 schema: {
 querystring: {
 type: "object",
 required: ["price"],
 properties: {
 price: { type: "number" }
 }
 }
 },
 handler: async (req, res) => {
 try {
 const { price } = req.query;

 await fastify.mysql.query(
 "UPDATE token_sales SET price = ? WHERE status = 'active'",
 [price]
 );

 return res.code(200).send({
 status: true,
 msg: "Price Updated"
 });
 } catch (err) {
 fastify.log.error("price update error:", err);
 return res.code(500).send({ status: false, msg: "Error updating price" });
 }
 }
 });
 // Fetch on-chain price from configured RPC and update active sale's price
 fastify.get('/fetchAndUpdatePrice', async (req, res) => {
 try {
 const rows = await fastify.mysql.query("SELECT * FROM settings LIMIT 1");
 if (!rows || rows.length === 0) {
 return res.code(400).send({ status: false, msg: 'No settings found (contract address missing)' });
 }
 const settings = rows[0];
 const contractAddress = settings.contract_address;
 if (!contractAddress) {
 return res.code(400).send({ status: false, msg: 'Contract address not configured in settings' });
 }

 const ethers = require('ethers');

 const simpleAbi = [
 {
 "inputs": [],
 "name": "tokenAmountPerUSD",
 "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
 "stateMutability": "view",
 "type": "function"
 }
 ];

 const rpcUrl = process.env.RPC_URL || null;
 let provider;
 if (rpcUrl) {
 provider = new ethers.providers.JsonRpcProvider(rpcUrl);
 } else {
 provider = ethers.getDefaultProvider();
 }

 const contract = new ethers.Contract(contractAddress, simpleAbi, provider);
 const onChain = await contract.tokenAmountPerUSD();
 const priceFloat = parseFloat(onChain.toString()) / (10 ** 18);

 // Update price for active sale(s)
 await fastify.mysql.query(
 "UPDATE token_sales SET price = ? WHERE status = 'active'",
 [priceFloat]
 );

 return res.code(200).send({ status: true, msg: 'Fetched and updated price', price: priceFloat });
 } catch (err) {
 fastify.log.error('fetchAndUpdatePrice error:', err);
 return res.code(500).send({ status: false, msg: 'Failed to fetch or update price', err: err.message });
 }
 });
 fastify.get('/logWithdraw', {
 schema: {
 querystring: {
 type: "object",
 required: ["coin", "amount", "to", "txHash"],
 properties: {
 coin: { type: "string" },
 amount: { type: "string" },
 to: { type: "string" },
 txHash: { type: "string" }
 }
 }
 }
 }, async (req, res) => {
 try {
 const { coin, amount, to, txHash } = req.query;

 const result = await fastify.mysql.query(
 'INSERT INTO admin_withdraw (coin, amount, to_address, tx_hash) VALUES (?, ?, ?, ?)',
 [coin, amount, to, txHash]
 );

 return res.code(200).send({ status: true, msg: "Withdraw logged successfully" });
 } catch (err) {
 fastify.log.error("Withdraw log error:", err);
 return res.code(500).send({ status: false, msg: "Failed to log withdraw" });
 }
 });

 fastify.get('/getTokensAvailable', async (req, res) => {
 try {
 const rows = await fastify.mysql.query("SELECT SUM(ptc_tokens) AS total_tokens FROM ico_purchases WHERE status = 'success'");
 return res.code(200).send({ status: true, data: rows[0] });
 } catch (err) {
 fastify.log.error("Withdraw log error:", err);
 return res.code(500).send({ status: false, msg: "Failed to log withdraw" });
 }
 })

 fastify.get('/getTokensAvailableForSale', async (req, res) => {
 try {
 let { start_at, end_at, total_sale_quantity } = req.query || {};

 if (!start_at || !end_at) {
 const rows = await fastify.mysql.query(`
 SELECT *
 FROM token_sales
 WHERE status = 'active' OR (NOW() BETWEEN start_at AND end_at)
 ORDER BY CASE WHEN status='active' THEN 0 ELSE 1 END, start_at DESC
 LIMIT 1
 `);

 const active = rows?.[0];

 if (!active) {
 return res.code(200).send({
 status: false,
 total_tokens_sold: 0,
 total_sale_quantity: 0,
 available_tokens: 0
 });
 }

 start_at = active.start_at;
 end_at = active.end_at;
 total_sale_quantity = active.token_quantity;
 }

 if (!start_at || !end_at) {
 return res.code(200).send({
 status: false,
 total_tokens_sold: 0,
 total_sale_quantity: 0,
 available_tokens: 0
 });
 }

 const soldRows = await fastify.mysql.query(
 `
 SELECT COALESCE(SUM(CAST(ptc_tokens AS DECIMAL(30,8))), 0) AS total_tokens_sold
 FROM ico_purchases
 WHERE LOWER(TRIM(status)) = 'success'
 AND created_at BETWEEN ? AND ?
 `,
 [start_at, end_at]
 );

 const sold = parseFloat(soldRows?.[0]?.total_tokens_sold || 0);
 const totalQty = Number(total_sale_quantity || 0);
 const available = Math.max(0, totalQty - sold);

 return res.code(200).send({
 status: true,
 total_tokens_sold: sold,
 total_sale_quantity: totalQty,
 available_tokens: available
 });

 } catch (err) {
 return res.code(500).send({
 status: false,
 msg: 'Failed to fetch sale tokens',
 error: err.message
 });
 }
 });


 fastify.get('/getWithdrawHistory', async (req, res) => {

 try {
 const data = await fastify.mysql.query(
 'select * from admin_withdraw'
 );

 return res.code(200).send({
 status: true,
 data: data
 });
 } catch (err) {
 console.error(err);
 return res.code(500).send({
 status: false,
 msg: "Transaction Failed",
 error: err.message
 });
 }
 });
 fastify.get('/getAllReferralData', async (request, reply) => {
 try {
 const allUsers = await fastify.mysql.query('SELECT * FROM ico_purchases where referrer_bonus > 0');
 return reply.code(200).send({
 status: true,
 data: allUsers,
 });
 } catch (err) {
 console.error(err);
 return reply.code(500).send({ status: false, msg: 'Internal Server Error' });
 }
 });

 // Get payment settings history
 fastify.get('/getPaymentSettingsHistory', async (request, reply) => {
 try {
 console.log('Fetching payment settings history...');
 const records = await fastify.mysql.query(`
 SELECT * FROM payment_settings_history
 ORDER BY timestamp DESC
 LIMIT 100
 `);

 console.log('History records found:', records ? records.length : 0, records);

 return reply.code(200).send({
 status: true,
 data: records || []
 });
 } catch (err) {
 console.error('Error fetching payment settings history:', err);
 fastify.log.error('Error fetching payment settings history:', err);
 return reply.code(500).send({
 status: false,
 msg: 'Internal Server Error',
 error: err.message
 });
 }
 });

 // Update token price with history logging
 fastify.post('/updateTokenPrice', {
 schema: {
 querystring: {
 type: 'object',
 properties: {
 price: { type: 'number' },
 txHash: { type: 'string' }
 }
 }
 }
 }, async (request, reply) => {
 try {
 const { price, txHash } = request.query;
 console.log('updateTokenPrice called with price:', price, 'txHash:', txHash);

 if (!price && price !== 0) {
 return reply.code(400).send({
 status: false,
 msg: 'Price parameter is required'
 });
 }

 // Get current price before update
 const currentRows = await fastify.mysql.query(
 "SELECT price FROM token_sales WHERE status = 'active' LIMIT 1"
 );
 const oldPrice = currentRows && currentRows.length > 0 ? currentRows[0].price : null;
 console.log('Old price:', oldPrice);

 // Update active sale price
 const updateResult = await fastify.mysql.query(
 "UPDATE token_sales SET price = ? WHERE status = 'active'",
 [price]
 );

 console.log('Update result:', updateResult);
 console.log('Update result:', updateResult);

 // Ensure history is logged even if no active sale row was updated.
 const changedByAddress = request.headers['x-user-address'] || 'admin@ptc.com';
 if (updateResult && updateResult.affectedRows > 0) {
 // Successful update of active sale(s) — insert history with previous value
 console.log('Inserting history record for:', changedByAddress);
 const insertResult = await fastify.mysql.query(
 `INSERT INTO payment_settings_history
 (setting_key, old_value, new_value, changed_by, transaction_hash, timestamp)
 VALUES (?, ?, ?, ?, ?, NOW())`,
 [
 'token_price',
 oldPrice ? oldPrice.toString() : null,
 price.toString(),
 changedByAddress,
 txHash || null
 ]
 );
 console.log('Insert history result:', insertResult);
 } else {
 // No active sale updated — try to update the most recent sale, and still log history
 try {
 const lastSaleRows = await fastify.mysql.query(
 "SELECT id, price FROM token_sales ORDER BY start_at DESC LIMIT 1"
 );
 const lastSale = lastSaleRows && lastSaleRows.length ? lastSaleRows[0] : null;
 if (lastSale) {
 const lastOldPrice = lastSale.price || null;
 const upd = await fastify.mysql.query(
 "UPDATE token_sales SET price = ? WHERE id = ?",
 [price, lastSale.id]
 );
 console.log('Updated most recent sale result:', upd);
 const insertResult = await fastify.mysql.query(
 `INSERT INTO payment_settings_history
 (setting_key, old_value, new_value, changed_by, transaction_hash, timestamp)
 VALUES (?, ?, ?, ?, ?, NOW())`,
 [
 'token_price',
 lastOldPrice ? lastOldPrice.toString() : null,
 price.toString(),
 changedByAddress,
 txHash || null
 ]
 );
 console.log('Insert history (fallback) result:', insertResult);
 } else {
 // No sale rows at all — still insert a history record to capture the change
 const insertResult = await fastify.mysql.query(
 `INSERT INTO payment_settings_history
 (setting_key, old_value, new_value, changed_by, transaction_hash, timestamp)
 VALUES (?, ?, ?, ?, ?, NOW())`,
 [
 'token_price',
 oldPrice ? oldPrice.toString() : null,
 price.toString(),
 changedByAddress,
 txHash || null
 ]
 );
 console.log('Inserted history with no sale present:', insertResult);
 }
 } catch (fallbackErr) {
 fastify.log.error('Error in fallback update/insert history:', fallbackErr);
 }
 }

 return reply.code(200).send({
 status: true,
 msg: 'Price updated and history logged'
 });
 } catch (err) {
 console.error('Error updating token price:', err);
 fastify.log.error('Error updating token price:', err);
 return reply.code(500).send({
 status: false,
 msg: 'Error updating price',
 error: err.message
 });
 }
 });



};
