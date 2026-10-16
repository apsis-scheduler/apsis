import Vue from 'vue'
import router from '@/router'
import { processMsgs } from '@/runs.js'
import store from '@/store.js'
import { JOBS_SCOPE, jobScope, runScope, setSummaryScope } from '@/summary.js'

jest.mock('@/summary.js', () => Object.assign(
  {}, require.requireActual('@/summary.js'), {setSummaryScope: jest.fn()},
))
jest.mock('@/websocket.js', () => ({
  Socket: class { open() {} close() {} },
}))

const flush = () => new Promise(resolve => setTimeout(resolve))

function windowScope(show) {
  return {window: [['time', 'now'], ['limit', show], ['grouping', 1]]}
}

function makeRun(run_id, seq) {
  return {run_id, job_id: 'job/' + run_id, args: {}, state: 'new', times: {}, seq,
          dependencies: [['dep/' + run_id, {}]]}
}

// resolves the oldest pending run fetch with `run`
function respond(run) {
  fetches.shift()({ok: true, json: async () => ({runs: {[run.run_id]: run}})})
  return flush()
}

let fetches, vm

beforeEach(async () => {
  // a path no view matches
  await router.push('/none').catch(() => {})
  setSummaryScope.mockClear()
  store.state.runs = new Map()
  store.state.jobs = new Map()
  fetches = []
  global.fetch = jest.fn(() => new Promise(resolve => fetches.push(resolve)))
  vm = new Vue({router, render: h => h('router-view')}).$mount()
})

afterEach(() => {
  vm.$destroy()
})

async function go(path) {
  await router.push(path)
  return flush()
}

describe('view summary scopes', () => {
  test('jobs, agents, and runs pages', async () => {
    await go('/jobs')
    await go('/agents')
    await go('/runs')
    expect(setSummaryScope.mock.calls).toEqual([[JOBS_SCOPE], [JOBS_SCOPE], [windowScope(50)]])
  })

  test('the runs page follows its query and reloads a stale window', async () => {
    await go('/runs')
    await go('/runs?show=20')
    processMsgs([{type: 'run_window', groups: {}, earlier: null, later: null}], store.state)
    store.state.runWindowStale = true
    await flush()
    store.state.runWindowStale = false
    await flush()
    expect(setSummaryScope.mock.calls)
      .toEqual([[windowScope(50)], [windowScope(20)], [windowScope(20), true]])
  })

  test('the job page rescopes when the job changes', async () => {
    store.state.summaryTruncated = {'job/b': 20}
    await go('/job/job/a')
    expect(vm.$el.textContent).not.toContain('Showing the newest')
    await go('/job/job/b')
    expect(vm.$el.textContent).toContain('Showing the newest 20 runs.')
    expect(setSummaryScope.mock.calls).toEqual([[jobScope('job/a')], [jobScope('job/b')]])
  })

  test('the run page keeps the previous scope until the run is known', async () => {
    const r1 = makeRun('r1', 1)
    const r2 = makeRun('r2', 1)
    await go('/runs/r1')
    expect(setSummaryScope).not.toHaveBeenCalled()
    await respond(r1)
    expect(setSummaryScope.mock.calls).toEqual([[runScope(r1)]])
    await go('/runs/r2')
    expect(setSummaryScope).toHaveBeenCalledTimes(1)
    await respond(r2)
    expect(setSummaryScope.mock.calls).toEqual([[runScope(r1)], [runScope(r2)]])
  })

  test('the run page refetches the run when its summary changes', async () => {
    await go('/runs/r3')
    expect(fetch).toHaveBeenCalledTimes(1)
    // the summary arrives before the fetch
    store.state.runs = new Map([['r3', makeRun('r3', 1)]])
    await flush()
    expect(fetch).toHaveBeenCalledTimes(2)
    await respond(makeRun('r3', 1))
    await respond(makeRun('r3', 1))

    store.state.runs = new Map([['r3', makeRun('r3', 2)]])
    await flush()
    expect(fetch).toHaveBeenCalledTimes(3)
    await respond(makeRun('r3', 2))

    // rescoping clears the run from the store then sends it again unchanged
    store.state.runs = new Map()
    await flush()
    store.state.runs = new Map([['r3', makeRun('r3', 2)]])
    await flush()
    expect(fetch).toHaveBeenCalledTimes(3)
  })
})
