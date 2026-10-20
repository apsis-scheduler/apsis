import * as runs from '@/runs.js'

test('a counted run sent already running', () => {
  const state = {runs: new Map(), jobs: new Map(), agentConns: new Map(), runWindow: null, runWindowStale: false}
  // the window counted r1 as the first of 5 scheduled runs, then r1 started before its summary was read
  runs.processMsgs([
    {type: 'run_summary', run_summary: {run_id: 'r1', job_id: 'a', args: {}, state: 'running',
      times: {schedule: '2026-10-14T10:00:00+00:00'}}},
    {type: 'run_window', anchor: '2026-10-14T09:59:59+00:00', groups: {r1: 5}, earlier: null,
      later: {time: '2026-10-14T11:00:00+00:00'}},
  ], state)
  // a later transition keeps the running key, so nothing marks the window stale
  runs.processMsgs([{type: 'run_transition', run_summary: {run_id: 'r1', job_id: 'a', args: {}, state: 'running',
    times: {schedule: '2026-10-14T10:00:00+00:00', running: '2026-10-14T10:00:01+00:00'}}}], state)
  console.log('count', runs.groupCount([state.runs.get('r1')], state.runWindow), 'stale', state.runWindowStale)
  expect(state.runWindowStale).toBe(true)
})
