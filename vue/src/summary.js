import { sortBy, uniq } from 'lodash'

import * as api from '@/api'
import { clearRunState, processMsgs } from '@/runs.js'
import store from '@/store.js'
import { Socket } from '@/websocket.js'

// scope of the jobs and agents views
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

let socket = null
let socketUrl = null
// true once the server closed the socket
let socketClosed = false
let generation = 0

/**
 * Subscribes the store to summary messages in `scope`, or to all messages if
 * no scope is given.
 *
 * Does nothing if the scope is unchanged or the open socket is unscoped,
 * since that already has everything.  Otherwise closes the previous socket,
 * clears runs, jobs, and agent conns, and opens a new socket.
 */
export function setSummaryScope(scope) {
  const url = api.getSummaryUrl(true, scope)
  if (
    socket !== null && !socketClosed
    && (url.toString() === socketUrl || socketUrl === api.getSummaryUrl(true).toString())
  )
    return
  socketUrl = url.toString()
  socketClosed = false

  // callbacks from a replaced socket are ignored
  const gen = ++generation
  const current = () => gen === generation

  if (socket !== null)
    socket.close()
  clearRunState(store.state)

  socket = new Socket(
    url,
    // onMessage
    msg => {
      if (current())
        processMsgs(JSON.parse(msg.data), store.state)
    },
    // onConnect
    () => {
      if (current()) {
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
    () => {
      // warn the user that the socket is closed so that they can refresh the page
      // because we don't automatically reconnect
      if (current()) {
        socketClosed = true
        store.state.errors.push('websocket closed: refresh the page')
      }
    },
    // don't reconnect to avoid bug where browser can't keep up, apsis closes the
    // connection due to backpressure, then the browser continues the cycle by
    // requesting the full run summary history
    { reconnect: false },
  )
  socket.open()
}
