import Vue from 'vue'
import RunsList from '@/components/RunsList'
import { clearRunState, processMsgs } from '@/runs.js'
import store from '@/store.js'

jest.useFakeTimers()

const QUERY = {time: '2026-10-14T11:30:00+00:00', show: 50, grouping: true}

function run(run_id, job_id, state, hour) {
  return {run_id, job_id, args: {}, state, times: {schedule: `2026-10-14T${hour}:00:00+00:00`}}
}

function summaries(...runs) {
  return runs.map(r => ({type: 'run_summary', run_summary: r}))
}

function groups(windowed) {
  const vm = new (Vue.extend(RunsList))({propsData: {query: QUERY, windowed}})
  return vm.groups
}

describe('RunsList run window', () => {
  beforeEach(() => {
    clearRunState(store.state)
    processMsgs(summaries(run('r1', 'a', 'success', '10'), run('r2', 'b', 'scheduled', '12')), store.state)
    processMsgs([{
      type: 'run_window',
      groups: {r1: 4, r2: 3},
      earlier: {time: '2026-10-14T10:00:00+00:00'},
      later: {time: '2026-10-14T12:00:00+00:00'},
    }], store.state)
    // a newer run of a after init
    processMsgs(summaries(run('r4', 'a', 'success', '11')), store.state)
  })

  test('windowed adds server group counts and edges', () => {
    const g = groups(true)
    expect(g.groups.map(r => r.run_id)).toEqual(['r2', 'r4'])
    expect(g.counts).toEqual({r2: 3, r4: 5})
    expect([g.earlierMore, g.earlierTime]).toEqual([true, '2026-10-14T10:00:00+00:00'])
    expect([g.laterMore, g.laterTime]).toEqual([true, '2026-10-14T12:00:00+00:00'])
  })

  test('windowed shows no more runs once the server window has no edges', () => {
    processMsgs([{type: 'run_window', groups: {r1: 4, r2: 3}, earlier: null, later: null}], store.state)
    const g = groups(true)
    expect([g.earlierMore, g.laterMore]).toEqual([false, false])
  })

  test('not windowed uses local runs only', () => {
    const g = groups(false)
    expect(g.groups.map(r => r.run_id)).toEqual(['r2', 'r4'])
    expect(g.counts).toEqual({r2: 1, r4: 2})
    expect([g.earlierMore, g.laterMore]).toEqual([false, false])
  })
})
