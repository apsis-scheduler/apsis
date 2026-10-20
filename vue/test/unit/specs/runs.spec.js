import * as runs from '@/runs'

describe('Urls', () => {
  test('sortState sorts', () => {
    expect(runs.sortStates([])).toEqual([])
    expect(runs.sortStates(['success'])).toEqual(['success'])
    expect(runs.sortStates(['scheduled', 'waiting', 'running', 'failure'])).toEqual(['scheduled', 'waiting', 'running', 'failure'])
    expect(runs.sortStates(['running', 'waiting', 'failure', 'scheduled'])).toEqual(['scheduled', 'waiting', 'running', 'failure'])
  })
})

describe('processMsgs', () => {
  test('summary init done sets truncation', () => {
    const state = {runs: new Map(), jobs: new Map(), agentConns: new Map()}
    runs.processMsgs([{type: 'summary_init_done', truncated: {a: 10000}}], state)
    expect(state.summaryTruncated).toEqual({a: 10000})
    runs.clearRunState(state)
    expect(state.summaryTruncated).toEqual({})
  })
})

describe('run windows', () => {
  const run = (run_id, state) => ({
    run_id, job_id: 'a', args: {}, state, times: {schedule: '2026-10-14T00:00:00+00:00'},
  })

  test('group counts add the runs the window left out', () => {
    const state = {runs: new Map(), jobs: new Map(), agentConns: new Map(), runWindow: null, runWindowStale: false}
    runs.processMsgs([{type: 'run_summary', run_summary: run('r1', 'success')}], state)
    runs.processMsgs([{type: 'run_window', groups: {r1: 5, r9: 2}, earlier: null, later: null}], state)
    expect(state.runWindow.groups).toEqual({r1: {count: 5, key: 'Ca\0'}, r9: {count: 2, key: null}})
    expect(runs.groupCount([state.runs.get('r1')], state.runWindow)).toBe(5)
    expect(runs.groupCount([state.runs.get('r1')], null)).toBe(1)

    // a newer run of the group adds to the count
    runs.processMsgs([{type: 'run_summary', run_summary: run('r2', 'failure')}], state)
    expect(runs.groupCount([state.runs.get('r1'), state.runs.get('r2')], state.runWindow)).toBe(6)

    expect(state.runWindowStale).toBe(false)

    // a counted run that left its group counts alone and the window goes stale
    runs.processMsgs([{type: 'run_transition', run_summary: run('r1', 'running')}], state)
    expect(runs.groupCount([state.runs.get('r1')], state.runWindow)).toBe(1)
    expect(state.runWindowStale).toBe(true)

    runs.clearRunState(state)
    expect(state.runWindow).toBe(null)
    expect(state.runWindowStale).toBe(false)
  })

  test('a running run counted with others makes the window stale', () => {
    for (const [count, stale] of [[1, false], [5, true]]) {
      const state = {runs: new Map(), jobs: new Map(), agentConns: new Map(), runWindow: null, runWindowStale: false}
      runs.processMsgs([
        {type: 'run_summary', run_summary: run('r1', 'running')},
        {type: 'run_window', groups: {r1: count}, earlier: null, later: null},
      ], state)
      expect(state.runWindowStale).toBe(stale)
    }
  })

  test('deleting a run the window sent makes it stale', () => {
    const state = {runs: new Map(), jobs: new Map(), agentConns: new Map(), runWindow: null, runWindowStale: false}
    runs.processMsgs([
      {type: 'run_summary', run_summary: run('r1', 'scheduled')},
      {type: 'run_window', groups: {r1: 24}, earlier: null, later: null},
    ], state)
    runs.processMsgs([{type: 'run_delete', run_id: 'r9'}], state)
    expect(state.runWindowStale).toBe(false)

    // rescheduling replaces r1 with r2 at the same time
    runs.processMsgs([
      {type: 'run_delete', run_id: 'r1'},
      {type: 'run_summary', run_summary: run('r2', 'scheduled')},
    ], state)
    expect(state.runWindowStale).toBe(true)
  })
})

describe('run window span', () => {
  const run = (run_id, hour) => ({
    run_id, job_id: 'a', args: {}, state: 'success', times: {schedule: `2026-10-14T${hour}:00:00+00:00`},
  })
  const kept = state => Array.from(state.runs.keys()).sort()

  function load(earlier, later) {
    const state = {runs: new Map(), jobs: new Map(), agentConns: new Map(), runWindow: null, runWindowStale: false}
    runs.processMsgs([
      {type: 'run_summary', run_summary: run('r1', '10')},
      {type: 'run_window', groups: {}, earlier, later},
    ], state)
    return state
  }

  test('runs outside the span or unsent at its edges are dropped', () => {
    const state = load({time: '2026-10-14T10:00:00+00:00'}, {time: '2026-10-14T12:00:00+00:00'})
    const msgs = [run('r1', '10'), run('r2', '10'), run('r3', '11'), run('r4', '12'), run('r5', '08'), run('r6', '13')]
    runs.processMsgs(msgs.map(r => ({type: 'run_transition', run_summary: r})), state)
    expect(kept(state)).toEqual(['r1', 'r3'])
  })

  test('a side with nothing past it is open', () => {
    const state = load(null, null)
    runs.processMsgs([run('r5', '08'), run('r6', '13')].map(r => ({type: 'run_summary', run_summary: r})), state)
    expect(kept(state)).toEqual(['r1', 'r5', 'r6'])
  })
})

test('isRunWindowStale', () => {
  const anchor = '2026-10-14T10:00:00+00:00'
  // the client clock is half an hour ahead of the server
  const loaded = Date.parse('2026-10-14T10:30:00Z')
  const at = (iso, runWindow = {anchor, end: Date.parse('2026-10-14T12:00:00+00:00'), loaded}) => (
    {runWindow, runWindowStale: false, time: new Date(iso)}
  )

  // a window at now goes stale once the time since it loaded reaches half its later span
  expect(runs.isRunWindowStale(at('2026-10-14T11:29:59Z'), true)).toBe(false)
  expect(runs.isRunWindowStale(at('2026-10-14T11:30:00Z'), true)).toBe(true)
  expect(runs.isRunWindowStale(at('2026-10-14T11:30:00Z'), false)).toBe(false)

  // an open later side never goes stale with time
  expect(runs.isRunWindowStale(at('2026-10-15T00:00:00Z', {anchor, end: Infinity, loaded}), true)).toBe(false)

  // a window anchored well before the client clock is fresh when it arrives
  const state = {runs: new Map(), runWindow: null, runWindowStale: false, time: new Date()}
  runs.processMsgs([{type: 'run_window', anchor, groups: {}, earlier: null, later: {time: '2026-10-14T12:00:00+00:00'}}], state)
  expect(runs.isRunWindowStale(state, true)).toBe(false)
  state.time = new Date(Date.now() + 3600 * 1000)
  expect(runs.isRunWindowStale(state, true)).toBe(true)

  // a window missing runs is stale and no window is not
  expect(runs.isRunWindowStale({...at('2026-10-14T10:00:00Z'), runWindowStale: true}, false)).toBe(true)
  expect(runs.isRunWindowStale(at('2026-10-14T10:00:00Z', null), true)).toBe(false)
})
