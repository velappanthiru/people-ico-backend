// Time conversion utility using Luxon for timezone-aware parsing.
// Exports `toUtcISOString(value)` which returns an ISO string in UTC or `null` for invalid input.
const { DateTime } = require('luxon');

// Default timezone for interpreting naive/local dates. Can be overridden with env var.
const DEFAULT_TIMEZONE = process.env.DEFAULT_TIMEZONE || 'Asia/Kolkata';

function toUtcISOString(value) {
  if (value === null || value === undefined) return null;

  // Numbers treated as epoch milliseconds
  if (typeof value === 'number' && !Number.isNaN(value)) {
    return new Date(value).toISOString();
  }

  // Date objects: interpret in DEFAULT_TIMEZONE (keeps previous behavior of treating wall-clock as IST-like)
  if (value instanceof Date) {
    return DateTime.fromJSDate(value, { zone: DEFAULT_TIMEZONE }).toUTC().toISO();
  }

  const s = String(value).trim();
  if (!s) return null;

  // Attempt ISO parsing with zone info (handles Z, +05:30, etc.)
  let dt = DateTime.fromISO(s, { setZone: true });
  if (dt.isValid) return dt.toUTC().toISO();

  // Try common space-separated formats (e.g., "YYYY-MM-DD HH:mm:ss[.SSS]") interpreted in DEFAULT_TIMEZONE
  dt = DateTime.fromFormat(s, 'yyyy-MM-dd HH:mm:ss.SSS', { zone: DEFAULT_TIMEZONE });
  if (dt.isValid) return dt.toUTC().toISO();
  dt = DateTime.fromFormat(s, 'yyyy-MM-dd HH:mm:ss', { zone: DEFAULT_TIMEZONE });
  if (dt.isValid) return dt.toUTC().toISO();
  dt = DateTime.fromFormat(s, 'yyyy-MM-dd', { zone: DEFAULT_TIMEZONE });
  if (dt.isValid) return dt.toUTC().toISO();

  // Fallback: try native Date parsing and interpret result in DEFAULT_TIMEZONE
  const native = new Date(s);
  if (!Number.isNaN(native.getTime())) {
    return DateTime.fromJSDate(native, { zone: DEFAULT_TIMEZONE }).toUTC().toISO();
  }

  // Unparseable
  return null;
}

module.exports = {
  toUtcISOString,
};
