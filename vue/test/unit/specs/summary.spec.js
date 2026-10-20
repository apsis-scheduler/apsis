jest.mock('@/websocket.js', () => {
  const sockets = []
  class Socket {
    constructor(url, onMessage, onConnect, onErr, onClose, options) {
      Object.assign(this, {url, onMessage, onConnect, onErr, onClose, options})
      this.open = jest.fn()
      this.close = jest.fn()
      sockets.push(this)
    }
  }
  return {Socket, sockets}
})

const RUN = {run_id: 'r1', job_id: 'a', args: {}, state: 'success', times: {schedule: '2026-10-14T00:00:00Z'}}

const DONE = {type: 'summary_init_done', truncated: {}}

function frame(...msgs) {
  return {data: JSON.stringify(msgs)}
}

let api, store, summary, sockets

beforeEach(() => {
  jest.resetModules()
  jest.useFakeTimers()
  api = require('@/api')
  store = require('@/store.js').default
  summary = require('@/summary.js')
  sockets = require('@/websocket.js').sockets
})

describe('setSummaryScope', () => {
  test('same scope keeps the socket', () => {
    summary.setSummaryScope(summary.JOBS_SCOPE)
    summary.setSummaryScope({types: ['job', 'agent_conn']})
    expect(sockets.length).toBe(1)
    expect(sockets[0].open).toHaveBeenCalledTimes(1)
    expect(sockets[0].close).not.toHaveBeenCalled()
  })

  test('an unscoped socket is kept for narrower scopes', () => {
    summary.setSummaryScope()
    summary.setSummaryScope(summary.jobScope('a'))
    summary.setSummaryScope(summary.JOBS_SCOPE)
    expect(sockets.length).toBe(1)
    expect(sockets[0].close).not.toHaveBeenCalled()
  })

  test('refresh reopens the same scope but not no scope', () => {
    summary.setSummaryScope(summary.JOBS_SCOPE)
    sockets[0].onMessage(frame(DONE))
    summary.setSummaryScope(summary.JOBS_SCOPE, true)
    expect(sockets.length).toBe(2)
    expect(sockets[0].close).toHaveBeenCalled()
    summary.setSummaryScope()
    summary.setSummaryScope(undefined, true)
    expect(sockets.length).toBe(3)
  })

  test('a closed socket reopens for the same scope', () => {
    summary.setSummaryScope(summary.JOBS_SCOPE)
    sockets[0].onClose({code: 1006})
    summary.setSummaryScope(summary.JOBS_SCOPE)
    expect(sockets.length).toBe(2)
    expect(sockets[1].url.search).toBe('?init&types=job,agent_conn')
    expect(sockets[1].open).toHaveBeenCalled()
  })

  test('new scope closes, opens, and replaces the store once its init is done', () => {
    summary.setSummaryScope(summary.jobScope('a'))
    sockets[0].onMessage(frame({type: 'run_summary', run_summary: {...RUN}}))
    expect(store.state.runs.size).toBe(0)
    sockets[0].onMessage(frame(DONE))
    expect(Array.from(store.state.runs.keys())).toEqual(['r1'])

    summary.setSummaryScope(summary.jobScope('b'))
    expect(sockets[0].close).toHaveBeenCalled()
    expect(sockets.length).toBe(2)
    expect(sockets[1].url.search).toBe('?init&types=job,run&job_id=b')
    expect(sockets[1].open).toHaveBeenCalled()
    sockets[1].onConnect()
    sockets[1].onMessage(frame({type: 'run_summary', run_summary: {...RUN, run_id: 'r2', job_id: 'b'}}))
    expect(Array.from(store.state.runs.keys())).toEqual(['r1'])
    sockets[1].onMessage(frame({type: 'run_window', groups: {}, earlier: null, later: null}))
    expect(Array.from(store.state.runs.keys())).toEqual(['r2'])
    // after init, frames apply as they come
    sockets[1].onMessage(frame({type: 'run_summary', run_summary: {...RUN, run_id: 'r3', job_id: 'b'}}))
    expect(store.state.runs.size).toBe(2)
  })

  test('no scope clears on connect and applies frames as they come', () => {
    summary.setSummaryScope(summary.jobScope('a'))
    sockets[0].onMessage(frame({type: 'run_summary', run_summary: {...RUN}}, DONE))
    summary.setSummaryScope()
    expect(store.state.runs.size).toBe(1)
    sockets[1].onConnect()
    expect(store.state.runs.size).toBe(0)
    sockets[1].onMessage(frame({type: 'run_summary', run_summary: {...RUN, run_id: 'r2'}}))
    expect(store.state.runs.size).toBe(1)
  })

  test('replaced socket is ignored', () => {
    summary.setSummaryScope(summary.jobScope('a'))
    summary.setSummaryScope(summary.jobScope('b'))
    sockets[0].onMessage(frame({type: 'run_summary', run_summary: {...RUN}}, DONE))
    expect(store.state.runs.size).toBe(0)
    sockets[1].onMessage(frame({type: 'run_summary', run_summary: {...RUN, job_id: 'b'}}, DONE))
    expect(store.state.runs.size).toBe(1)
  })

  test('only the current socket reports errors', () => {
    summary.setSummaryScope(summary.jobScope('a'))
    summary.setSummaryScope()
    sockets[0].onErr()
    sockets[0].onClose({code: 1000})
    expect(store.state.errors.errors).toEqual([])

    sockets[1].onErr()
    sockets[1].onClose({code: 1006})
    expect(store.state.errors.errors.map(e => e.message))
      .toEqual(['connection error', 'websocket closed: refresh the page'])
    sockets[0].onConnect()
    expect(store.state.errors.errors.length).toBe(2)
    sockets[1].onConnect()
    expect(store.state.errors.errors).toEqual([])
  })
})

describe('run windows', () => {
  const QUERY = {
    path: null, keywords: null, labels: null, states: null, args: null,
    grouping: true, show: 50, time: 'now', asc: false,
  }

  test('query to window scope', () => {
    expect(summary.windowScope(QUERY).window)
      .toEqual([['time', 'now'], ['limit', 50], ['grouping', 1]])
    const query = {
      ...QUERY,
      path: 'a/b', keywords: ['x', 'y'], labels: ['l'], states: ['failure', 'error'],
      args: {d: '1', e: null}, grouping: false, show: 20, time: '2026-10-14T12:00:00+00:00',
    }
    expect(summary.windowScope(query).window).toEqual([
      ['time', '2026-10-14T12:00:00+00:00'], ['limit', 20], ['grouping', 0],
      ['path', 'a/b'], ['keyword', 'x'], ['keyword', 'y'], ['label', 'l'],
      ['state', 'failure'], ['state', 'error'], ['arg', 'd=1'], ['arg', 'e'],
    ])
    expect(api.getSummaryUrl(summary.windowScope(query)).search).toBe(
      '?init&time=2026-10-14T12%3A00%3A00%2B00%3A00&limit=20&grouping=0&path=a%2Fb'
      + '&keyword=x&keyword=y&label=l&state=failure&state=error&arg=d%3D1&arg=e'
    )
  })

  test('going back to now from an old window opens one socket', async () => {
    const Vue = require('vue')
    const router = require('@/router').default
    const vm = new Vue({router, render: h => h('router-view')}).$mount()
    await router.push('/runs?time=20261014T100000Z')
    sockets[sockets.length - 1].onMessage(frame({
      type: 'run_window', anchor: '2026-10-14T10:00:00+00:00', groups: {},
      earlier: null, later: {time: '2026-10-14T10:10:00+00:00'},
    }))
    // the old window is past half its later span
    store.state.time = new Date(Date.now() + 10 * 60 * 1000)
    await router.push('/runs')
    expect(sockets.slice(-2).map(s => s.url.search)).toEqual([
      '?init&time=2026-10-14T10%3A00%3A00Z&limit=50&grouping=1',
      '?init&time=now&limit=50&grouping=1',
    ])
    vm.$destroy()
  })

  test('4503 falls back to no scope', () => {
    summary.setSummaryScope(summary.windowScope(QUERY))
    sockets[0].onClose({code: 4503})
    expect(sockets.length).toBe(2)
    expect(sockets[1].url.search).toBe('?init')
    expect(store.state.errors.errors).toEqual([])
    summary.setSummaryScope(summary.windowScope({...QUERY, show: 20}))
    expect(sockets.length).toBe(2)
  })
})

test('summary urls', () => {
  const dependent = {...RUN, job_id: 'c', dependencies: [['b', {}], ['a', {d: '1'}], ['b', {d: '2'}]]}
  const cases = [
    [undefined, '?init'],
    [summary.JOBS_SCOPE, '?init&types=job,agent_conn'],
    [summary.jobScope('x/a b'), '?init&types=job,run&job_id=x%2Fa%20b'],
    [summary.runScope(RUN), '?init&types=run&job_id=a&dependents_of=a'],
    [summary.runScope(dependent), '?init&types=run&job_id=a&job_id=b&job_id=c&dependents_of=c'],
  ]
  for (const [scope, search] of cases) {
    const url = api.getSummaryUrl(scope)
    expect([url.protocol, url.search]).toEqual(['ws:', search])
  }
})
