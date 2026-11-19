jest.useFakeTimers()
const store = require('@/store').default

function setHidden(hidden) {
  Object.defineProperty(document, 'hidden', {value: hidden, configurable: true})
  document.dispatchEvent(new Event('visibilitychange'))
}

describe('store clock', () => {
  test('pauses while hidden and catches up when shown', () => {
    const stale = new Date(0)

    setHidden(true)
    store.state.time = stale
    jest.advanceTimersByTime(5000)
    expect(store.state.time).toBe(stale)

    setHidden(false)
    expect(store.state.time).not.toBe(stale)

    const shown = store.state.time
    jest.advanceTimersByTime(1000)
    expect(store.state.time).not.toBe(shown)
  })
})
