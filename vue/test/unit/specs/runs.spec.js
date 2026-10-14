import * as runs from '@/runs'

describe('Urls', () => {
  test('sortState sorts', () => {
    expect(runs.sortStates([])).toEqual([])
    expect(runs.sortStates(['success'])).toEqual(['success'])
    expect(runs.sortStates(['scheduled', 'waiting', 'running', 'failure'])).toEqual(['scheduled', 'waiting', 'running', 'failure'])
    expect(runs.sortStates(['running', 'waiting', 'failure', 'scheduled'])).toEqual(['scheduled', 'waiting', 'running', 'failure'])
  })
})

describe('applyRunSummary', () => {
  test('takes summary fields and keeps full run fields', () => {
    const run = {
      run_id: 'r1',
      state: 'scheduled',
      expected: true,
      conds: [{str: 'dep'}],
      actions: [],
      meta: {job: {labels: ['a']}},
      program: {str: 'echo'},
    }
    const summary = {run_id: 'r1', state: 'running', times: {running: 't'}}
    const merged = runs.applyRunSummary(run, summary)
    expect(merged).toEqual({
      run_id: 'r1',
      state: 'running',
      times: {running: 't'},
      conds: [{str: 'dep'}],
      actions: [],
      meta: {job: {labels: ['a']}},
      program: {str: 'echo'},
    })
    expect(Object.isFrozen(merged)).toBe(true)
  })
})
