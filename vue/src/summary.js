import { sortBy, uniq } from 'lodash'

import * as api from '@/api'
import { clearRunState, processMsgs } from '@/runs.js'
import store from '@/store.js'
import { Socket } from '@/websocket.js'

// shared by the jobs and agents views so switching between them keeps the socket
export const JOBS_SCOPE = {types: ['job', 'agent_conn']}

export function jobScope(job_id) {
  return {types: ['job', 'run'], job_ids: [job_id]}
}

/**
 * Returns the scope for a run's view, its job, the jobs it depends on, and
 * the jobs that depend on its job.
 */
export function runScope(run) {
  const deps = (run.dependencies || []).map(([job_id]) => job_id)
  return {
    types: ['run'],
    job_ids: sortBy(uniq([run.job_id, ...deps])),
    dependents_of: [run.job_id],
  }
}

/**
 * Returns run window params for a RunsList query.
 *
 * @returns an array of `[param, value]` pairs
 */
export function queryToWindowParams(query) {
  const params = [
    ['time', query.time],
    ['limit', query.show],
    ['grouping', query.grouping ? 1 : 0],
  ]
  if (query.path)
    params.push(['path', query.path])
  for (const keyword of query.keywords || [])
    params.push(['keyword', keyword])
  for (const label of query.labels || [])
    params.push(['label', label])
  for (const state of query.states || [])
    params.push(['state', state])
  for (const [param, value] of Object.entries(query.args || {}))
    params.push(['arg', value === null ? param : param + '=' + value])
  return params
}

export function windowScope(query) {
  return {window: queryToWindowParams(query)}
}

let socket = null
let socketUrl = null
// true once the server closed the socket
let socketClosed = false
let generation = 0
// set when the server can't serve run windows
let windowUnavailable = false

/**
 * Subscribes the store to summary messages in `scope`, or to all messages if
 * no scope is given.
 *
 * Does nothing if the scope is unchanged and `refresh` is false, or if the
 * open socket is unscoped, since that already has everything.  Otherwise closes the previous socket
 * and opens a new one, and replaces runs, jobs, and agent conns once its init
 * is done, or right away for no scope.  If the server can't serve run
 * windows, a window scope becomes no scope.
 */
export function setSummaryScope(scope, refresh = false) {
  if (windowUnavailable && scope && scope.window)
    scope = undefined
  const url = api.getSummaryUrl(true, scope)
  if (
    socket !== null && !socketClosed
    && ((url.toString() === socketUrl && !refresh) || socketUrl === api.getSummaryUrl(true).toString())
  )
    return
  socketUrl = url.toString()
  socketClosed = false
  // init frames held until the init is done, or null to apply frames as they come
  let held = url.toString() === api.getSummaryUrl(true).toString() ? null : []

  // callbacks from a replaced socket are ignored
  const gen = ++generation
  const current = () => gen === generation

  if (socket !== null)
    socket.close()

  socket = new Socket(
    url,
    // onMessage
    msg => {
      if (!current())
        return
      const msgs = JSON.parse(msg.data)
      if (held === null)
        processMsgs(msgs, store.state)
      else {
        held.push(msgs)
        if (msgs.some(m => m.type === 'run_window' || m.type === 'summary_init_done')) {
          clearRunState(store.state)
          processMsgs([].concat(...held), store.state)
          held = null
        }
      }
    },
    // onConnect
    () => {
      if (current()) {
        if (held === null)
          clearRunState(store.state)
        store.state.errors.pop('connection error')
        store.state.errors.pop('websocket closed: refresh the page')
      }
    },
    // onError
    () => {
      if (current())
        store.state.errors.push('connection error')
    },
    // onClose
    event => {
      if (!current())
        return
      socketClosed = true
      if (event.code === 4503) {
        windowUnavailable = true
        setSummaryScope(scope)
      }
      else
        // warn the user that the socket is closed so that they can refresh the page
        // because we don't automatically reconnect
        store.state.errors.push('websocket closed: refresh the page')
    },
    // don't reconnect to avoid bug where browser can't keep up, apsis closes the
    // connection due to backpressure, then the browser continues the cycle by
    // requesting the full run summary history
    { reconnect: false },
  )
  socket.open()
}
