import Vue from 'vue'
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
  const summary = (run_id, state) => ({
    type: 'run_summary',
    run_summary: {run_id, job_id: 'job', args: {}, state, times: {schedule: '2026-10-14T00:00:00Z'}},
  })

  test('updates runs watched by vue', () => {
    const state = {}
    runs.clearRunState(state)
    const vm = new Vue({
      data: {state},
      computed: {
        runStates() {
          return Array.from(this.state.runs.values()).map(r => r.run_id + ':' + r.state)
        },
      },
    })

    runs.processMsgs([summary('r1', 'scheduled'), summary('r2', 'scheduled')], state)
    expect(vm.runStates).toEqual(['r1:scheduled', 'r2:scheduled'])

    runs.processMsgs([summary('r1', 'running')], state)
    expect(vm.runStates).toEqual(['r1:running', 'r2:scheduled'])

    runs.processMsgs([{type: 'run_delete', run_id: 'r2'}], state)
    expect(vm.runStates).toEqual(['r1:running'])
    expect(state.runs.size).toBe(1)
    expect(state.runs.get('r1').state).toBe('running')
  })
})
