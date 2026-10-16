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
