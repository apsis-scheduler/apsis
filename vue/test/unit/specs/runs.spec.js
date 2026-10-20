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

describe('filters', () => {
  test('toggleLabel adds a missing label and removes a present one', () => {
    expect(runs.toggleLabel(null, 'foo')).toEqual(['foo'])
    expect(runs.toggleLabel(['foo'], 'bar')).toEqual(['foo', 'bar'])
    expect(runs.toggleLabel(['foo', 'bar'], 'foo')).toEqual(['bar'])
    expect(runs.toggleLabel(['foo'], 'foo')).toBe(null)
  })

  test('toggleArg sets a param and removes it when it already has that arg', () => {
    expect(runs.toggleArg(null, 'fruit', 'mango')).toEqual({fruit: 'mango'})
    expect(runs.toggleArg({fruit: 'mango'}, 'fruit', 'kiwi')).toEqual({fruit: 'kiwi'})
    expect(runs.toggleArg({fruit: null}, 'fruit', 'kiwi')).toEqual({fruit: 'kiwi'})
    expect(runs.toggleArg({fruit: 'mango', color: 'red'}, 'fruit', 'mango')).toEqual({color: 'red'})
    expect(runs.toggleArg({fruit: 'mango'}, 'fruit', 'mango')).toBe(null)
  })
})
