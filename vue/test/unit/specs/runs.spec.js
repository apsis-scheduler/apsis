import * as runs from '@/runs'

describe('Urls', () => {
  test('sortState sorts', () => {
    expect(runs.sortStates([])).toEqual([])
    expect(runs.sortStates(['success'])).toEqual(['success'])
    expect(runs.sortStates(['scheduled', 'waiting', 'running', 'failure'])).toEqual(['scheduled', 'waiting', 'running', 'failure'])
    expect(runs.sortStates(['running', 'waiting', 'failure', 'scheduled'])).toEqual(['scheduled', 'waiting', 'running', 'failure'])
  })
})

test('toggleLabel adds to no filter and empties to null', () => {
  expect(runs.toggleLabel(null, 'a')).toEqual(['a'])
  expect(runs.toggleLabel(['a'], 'a')).toBe(null)
})
