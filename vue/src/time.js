import moment from 'moment-timezone'

const TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

export function formatTime(time, tz, format) {
  if (time)
    return moment(time).tz(tz).format(format || TIME_FORMAT)
  else
    return ''
}

export function formatDuration(elapsed) {
  return (
      elapsed < 0 ? '??'
    : elapsed === 0   ? '0 s'
    : elapsed < 1e-5  ? Math.round(elapsed *   1e6) + ' µs'
    : elapsed < 1e-4  ? Math.round(elapsed *   1e6) + ' µs'
    : elapsed < 1e-3  ? Math.round(elapsed *   1e6) + ' µs'
    : elapsed < 1e-2  ? Math.round(elapsed *   1e3) + ' ms'
    : elapsed < 1e-1  ? Math.round(elapsed *   1e3) + ' ms'
    : elapsed < 1     ? Math.round(elapsed *   1e3) + ' ms'
    : elapsed < 60    ? Math.round(elapsed        ) + ' s'
    : elapsed < 3600  ? Math.round(elapsed /    60) + ' m'
    : elapsed < 86400 ? Math.round(elapsed /  3600) + ' h'
    :                   Math.round(elapsed / 86400) + ' d'
  )
}

export function formatElapsed(elapsed) {
  return (
      elapsed < 3600 ? 
          Math.trunc(elapsed / 60) 
        + ':' + ('' + Math.round(elapsed % 60)).padStart(2, '0')
    :     Math.trunc(elapsed / 3600) 
        + ':' + ('' + Math.trunc(elapsed / 60 % 60)).padStart(2, '0')
        + ':' + ('' + Math.round(elapsed % 60)).padStart(2, '0')
  )
}

export function parseDate(str, timeZone) {
  str = str.trim()

  if (str === 'today' || str === 'yesterday' || str === 'tomorrow') {
    let now = moment().tz(timeZone)
    if (str === 'yesterday')
      now.add(-1, 'day')
    if (str === 'tomorrow')
      now.add(1, 'day')
    return [now.year(), now.month(), now.date()]
  }

  let match = str.match(/^(\d\d\d\d)-(\d\d)-(\d\d)$/)
  if (match !== null)
    return [parseInt(match[1]), parseInt(match[2]) - 1, parseInt(match[3])]

  match = str.match(/^(\d\d\d\d)(\d\d)(\d\d)$/)
  if (match !== null)
    return [parseInt(match[1]), parseInt(match[2]) - 1, parseInt(match[3])]

  return null
}

export function parseDaytime(str) {
  str = str.trim()

  let match = str.match(/^(\d\d?):(\d\d)(?::(\d\d))?$/)
  if (match === null)
    match = str.match(/^(\d\d?)(\d\d)(?:(\d\d))?$/)
  if (match !== null) {
    const h = parseInt(match[1])
    const m = parseInt(match[2])
    const s = match[3] === undefined ? 0 : parseInt(match[3])
    return [h, m, s]
  }

  return null
}

export function parseTime(str, end, timeZone) {
  const parts = str.trim().split(/ +/)

  if (parts.length === 0 || parts.length > 2)
    return null

  let date = parseDate(parts[0], timeZone)
  if (date === null) {
    // Try to parse it as a time in today's date.
    const daytime = parseDaytime(parts[0])
    if (daytime === null)
      return null
    const now = moment(new Date()).tz(timeZone)
    const x = [now.year(), now.month(), now.date()].concat(daytime)
    return moment.tz(x, timeZone)
  }

  if (parts.length === 2) {
    const daytime = parseDaytime(parts[1])
    return daytime === null ? null : moment.tz(date.concat(daytime), timeZone)
  }
  else {
    date = moment.tz(date, timeZone)
    if (end)
      date.add(1, 'days')
    return date
  }
}

/**
 * Parses a time offset and applies it to time.
 * 
 * Understands,
 * - "#w" where # is number of weeks.
 * - "#d" where # is number of days.
 * - "#h" where # is number of hours.
 * - "#m" where # is number of minutes.
 * - "#s" where # is number of seconds.
 * 
 * @param {} str - string to parse
 * @param {*} sign - sign for date offset
 * @param {*} time - time to offset from or now if null
 */
export function parseTimeOffset(str, sign, time) {
  if (!time)
    time = new Date()
  const match = str.match(/^(\d+)([wdhms])$/)
  if (match !== null) {
    if (match[2] === 'w')
      time.setDate(time.getDate() + sign * 7 * parseInt(match[1]))
    else if (match[2] === 'd')
      time.setDate(time.getDate() + sign * parseInt(match[1]))
    else if (match[2] === 'h')
      time.setTime(time.getTime() + sign * 3600 * 1000 * parseInt(match[1]))
    else if (match[2] === 'm')
      time.setTime(time.getTime() + sign * 60 * 1000 * parseInt(match[1]))
    else if (match[2] === 's')
      time.setTime(time.getTime() + sign * 1000 * parseInt(match[1]))
    return time
  }
  return null
}

export function parseTimeOrOffset(str, end, timeZone) {
  let time = parseTime(str, end, timeZone)
  if (time === null)
    time = parseTimeOffset(str, end ? 1 : -1)
  return time
}

// Converts a full ISO time string to a compact UTC with 1 s resolution,
// e.g. 20230119T123304Z.
export function formatCompactUTCTime(str) {
  const time = moment.utc(str).tz('UTC')
  return time.format('YYYYMMDD[T]HHmmss[Z]')
}

// Converts compact UTC to full ISO-8859 UTC.
export function parseCompactUTCTime(str) {
  return moment.utc(str).format()
}

const UNIT_MS = { s: 1000, m: 60000, h: 3600000, d: 86400000 }

/**
 * Returns true if `expr` is a relative time expression like "now", "now-30m",
 * or "now+2h".
 */
export function isRelativeTimeExpr(expr) {
  return expr === 'now' || /^now[+-]\d+[smhd]$/.test(expr)
}

/**
 * Resolves a time expression to an ISO timestamp string.
 *
 * Accepts:
 * - "now" → current time
 * - "now-30m", "now-7d", etc. → relative offset in the past (s, m, h, d)
 * - "now+1h", "now+2d", etc. → relative offset in the future (s, m, h, d)
 * - null → null (no bound)
 * - ISO timestamp string → returned as-is
 */
export function resolveTimeExpr(expr) {
  if (!expr) return null
  if (expr === 'now') return new Date().toISOString()
  const match = expr.match(/^now([+-])(\d+)([smhd])$/)
  if (match) {
    const sign = match[1] === '+' ? 1 : -1
    return new Date(Date.now() + sign * parseInt(match[2]) * UNIT_MS[match[3]]).toISOString()
  }
  return expr
}

/**
 * Formats a time expression for display.
 *
 * Relative expressions ("now", "now-30m") are shown as-is.
 * Absolute timestamps are formatted in the given timezone.
 */
export function formatTimeExpr(expr, timeZone) {
  if (!expr) return ''
  if (isRelativeTimeExpr(expr)) return expr
  return formatTime(new Date(expr), timeZone)
}

