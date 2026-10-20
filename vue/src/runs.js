import { isEqual, sortBy, toPairs } from 'lodash'

export function joinArgs(args) {
  return toPairs(args).map(([n, v]) => n + '=' + v).join(', ')
}

const RUN_STATE_GROUPS = {
  'new': 'S',
  'scheduled': 'S',
  'waiting': 'R',
  'starting': 'R',
  'running': 'R',
  'stopping': 'R',
  'success': 'C',
  'failure': 'C',
  'error': 'C',
  'skipped': 'C',
}

export function isComplete(state) {
  return RUN_STATE_GROUPS[state] === 'C'
}

function groupKey(run) {
  const sgrp = RUN_STATE_GROUPS[run.state]
  return sgrp + (
    sgrp === 'R'
    // Waiting and running runs are never grouped.  
    ? run.run_id
    // Runs in other state are grouped by instance.
    : run.job_id + '\0' + Object.values(run.args).join('\0')
  )
}

/**
 * Returns the key by which runs are time-sorted.
 */
function timeKey(run) {
  return run.times.schedule || run.times.running || run.times.error
}

let nextSeq = 0

export function clearRunState(state) {
  state.runs = new Map()
  state.jobs = new Map()
  state.agentConns = new Map()
}

/**
 * Updates store `state` with runs from run summaries in `msgs`.
 */
export function processMsgs(msgs, state) {
  const seq = nextSeq++

  let runs = null
  let jobs = null
  let agentConns = null

  let stats = {job: 0, agentConn: 0, run: 0}

  // Pre-sort by time, to keep future sorts quick.
  msgs = sortBy(msgs, m => m.run_summary && timeKey(m.run_summary))

  for (const msg of msgs)
    switch (msg.type) {
      case 'agent_conn':
        if (agentConns === null)
          agentConns = new Map(state.agentConns)
        agentConns.set(msg.conn.info.conn.conn_id, msg.conn)
        stats.agentConn++
        break

      case 'agent_conn_delete':
        if (agentConns === null)
          agentConns = new Map(state.agentConns)
        agentConns.delete(msg.conn_id)
        stats.agentConn++
        break

      // Treat new and changed jobs the same.
      case 'job':
      case 'job_add':
        if (jobs === null)
          jobs = new Map(state.jobs)
        jobs.set(msg.job.job_id, Object.freeze(msg.job))
        stats.job++
        break

      case 'job_delete':
        if (jobs === null)
          jobs = new Map(state.jobs)
        jobs.delete(msg.job_id)
        stats.job++
        break

      case 'run_summary':
      case 'run_transition':
        if (runs === null)
          runs = new Map(state.runs)
        let run = msg.run_summary
        // Add sort and group keys to runs in msg.
        run.group_key = groupKey(run)
        run.time_key = timeKey(run)
        run.seq = seq
        // Build an instance key for quick determination of reruns.
        // We never change the runs, so freeze them to avoid reactivity.
        runs.set(run.run_id, Object.freeze(run))
        stats.run++
        break

      case 'run_delete':
        if (runs === null)
          runs = new Map(state.runs)
        runs.delete(msg.run_id)
        stats.run++
        break
    }

  // Set new maps to trigger reactivity updates.
  if (agentConns !== null)
    state.agentConns = agentConns
  if (jobs !== null)
    state.jobs = jobs
  if (runs !== null)
    state.runs = runs

  console.log(
    'summary msgs:',
    stats.agentConn, 'agentConn', stats.job, 'job', stats.run, 'run')
}

export const STATES = [
  'new',
  'scheduled',
  'waiting',
  'starting',
  'running',
  'stopping',
  'success',
  'failure',
  'skipped',
  'error',
]

export const OPERATIONS = {
  'new'         : [],
  'scheduled'   : ['start', 'skip'],
  'waiting'     : ['start', 'skip'],
  'starting'    : [],
  'running'     : ['terminate', 'kill', 'stop'],
  'stopping'    : ['terminate', 'kill'],
  'success'     : ['rerun', 'mark failure', 'mark skipped', 'mark error'],
  'failure'     : ['rerun', 'mark success', 'mark skipped', 'mark error'],
  'skipped'     : ['rerun', 'mark success', 'mark failure', 'mark error'],
  'error'       : ['rerun', 'mark success', 'mark failure', 'mark skipped'],
}

export function sortStates(states) {
  return sortBy(states, s => STATES.indexOf(s))
}

/**
 * Converts a run args dict to an array of PARAM=VALUE strings.
 */
export function argsToArray(args) {
  return Object.entries(args).map(([param, val]) => param + '=' + val)
}

/**
 * Converts an array of PARAM=VALUE strings to an args dict.
 */
export function arrayToArgs(arr) {
  const args = {}
  for (const el of arr) {
    const i = el.indexOf('=')
    if (i === -1)
      args[el] = null
    else
      args[el.slice(0, i)] = el.slice(i + 1)
  }
  return args
}

/**
 * Returns true if each member of `keywords` is a substring of `string`.
 */
export function matchKeywords(keywords, string) {
  for (const keyword of keywords)
    if (string.indexOf(keyword) === -1)
      return false
  return true
}

/**
 * Returns true if each members of `arr0` is in `arr1`.
 */
export function includesAll(arr0, arr1) {
  for (const el of arr0)
    if (!arr1.includes(el))
      return false
  return true
}

/**
 * Collects dependencies for `run`.
 *
 * @returns an array of dependency objects each including `job_id`, `args`, and
 * array of `runs`
 */
export function getDependencies(run, store) {
  return (run.dependencies || [])
    .map(([j, a]) => ({
      job_id: j,
      args: a,
      runs: store.getRunsForInstance(j, a),
    }))
}

/**
 * Collects dependents for `run`.
 *
 * @returns an array of runs for which `run` matches a dependency.
 */
export function getDependents(run, store) {
  let deps = new Map()

  let j, a
  for (let dep of store.state.runs.values())
    for (([j, a]) of (dep.dependencies || []))
      if (j === run.job_id && isEqual(a, run.args)) {
        const key = dep.job_id + '(' + joinArgs(dep.args) + ')'
        const dep_runs = deps.get(key)
        if (dep_runs)
          dep_runs.push(dep)
        else
          deps.set(key, [dep])
      }

  return Array.from(deps.values().map(runs => ({
    job_id: runs[0].job_id,
    args: runs[0].args,
    runs: runs,
  })))
}

