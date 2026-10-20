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

  test('a closed socket reopens for the same scope', () => {
    summary.setSummaryScope(summary.JOBS_SCOPE)
    sockets[0].onClose()
    summary.setSummaryScope(summary.JOBS_SCOPE)
    expect(sockets.length).toBe(2)
    expect(sockets[1].url.search).toBe('?init&types=job,agent_conn')
    expect(sockets[1].open).toHaveBeenCalled()
  })

  test('new scope closes, clears, and opens', () => {
    summary.setSummaryScope(summary.jobScope('a'))
    sockets[0].onMessage(frame({type: 'run_summary', run_summary: {...RUN}}))
    expect(store.state.runs.size).toBe(1)

    summary.setSummaryScope(summary.jobScope('b'))
    expect(sockets[0].close).toHaveBeenCalled()
    expect(store.state.runs.size).toBe(0)
    expect(sockets.length).toBe(2)
    expect(sockets[1].url.search).toBe('?init&types=job,run&job_id=b')
    expect(sockets[1].open).toHaveBeenCalled()
  })

  test('replaced socket is ignored', () => {
    summary.setSummaryScope(summary.jobScope('a'))
    summary.setSummaryScope(summary.jobScope('b'))
    sockets[0].onMessage(frame({type: 'run_summary', run_summary: {...RUN}}))
    expect(store.state.runs.size).toBe(0)
    sockets[1].onMessage(frame({type: 'run_summary', run_summary: {...RUN, job_id: 'b'}}))
    expect(store.state.runs.size).toBe(1)
  })

  test('only the current socket reports errors', () => {
    summary.setSummaryScope(summary.jobScope('a'))
    summary.setSummaryScope()
    sockets[0].onErr()
    sockets[0].onClose()
    expect(store.state.errors.errors).toEqual([])

    sockets[1].onErr()
    sockets[1].onClose()
    expect(store.state.errors.errors.map(e => e.message))
      .toEqual(['connection error', 'websocket closed: refresh the page'])
    sockets[0].onConnect()
    expect(store.state.errors.errors.length).toBe(2)
    sockets[1].onConnect()
    expect(store.state.errors.errors).toEqual([])
  })
})

test('summary urls', () => {
  const dependent = {...RUN, job_id: 'c', dependencies: [['b', {}], ['a', {d: '1'}], ['b', {d: '2'}]]}
  const cases = [
    [true, undefined, '?init'],
    [true, summary.JOBS_SCOPE, '?init&types=job,agent_conn'],
    [true, summary.jobScope('x/a b'), '?init&types=job,run&job_id=x%2Fa%20b'],
    [true, summary.runScope(RUN), '?init&types=run&job_id=a&dependents_of=a'],
    [true, summary.runScope(dependent), '?init&types=run&job_id=a&job_id=b&job_id=c&dependents_of=c'],
  ]
  for (const [init, scope, search] of cases) {
    const url = api.getSummaryUrl(init, scope)
    expect([url.protocol, url.search]).toEqual(['ws:', search])
  }
})
