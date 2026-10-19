import { columnWidth, dragWidth, loadWidths, MIN_WIDTH, saveWidths } from '@/columns'

function useStorage(storage) {
  Object.defineProperty(window, 'localStorage', {value: storage, configurable: true})
}

function memoryStorage() {
  const items = new Map()
  return {
    getItem: key => items.has(key) ? items.get(key) : null,
    setItem: (key, value) => items.set(key, String(value)),
  }
}

afterEach(() => {
  delete window.localStorage
})

describe('column widths', () => {
  test('saved widths load back and bad or missing storage loads none', () => {
    useStorage(memoryStorage())
    expect(loadWidths()).toEqual({})
    saveWidths({job: 500})
    expect(loadWidths()).toEqual({job: 500})

    localStorage.setItem('apsis.runsList.columnWidths', '{')
    expect(loadWidths()).toEqual({})
    localStorage.setItem('apsis.runsList.columnWidths', '3')
    expect(loadWidths()).toEqual({})
    localStorage.setItem('apsis.runsList.columnWidths', '[500]')
    expect(loadWidths()).toEqual({})

    useStorage({getItem: () => { throw new Error('denied') }, setItem: () => { throw new Error('full') }})
    expect(loadWidths()).toEqual({})
    saveWidths({job: 500})
  })

  test('a column takes its saved width at least the minimum else its default', () => {
    const widths = {job: 500, run: 1, args: 'wide'}
    expect(columnWidth(widths, 'job')).toBe(500)
    expect(columnWidth(widths, 'run')).toBe(MIN_WIDTH)
    expect(columnWidth(widths, 'args')).toBe(columnWidth({}, 'args'))
    expect(columnWidth({}, 'arg:date')).toBe(columnWidth({}, 'arg:other'))
  })

  test('dragging moves the right edge by whole pixels down to the minimum', () => {
    expect(dragWidth(200, 30.4)).toBe(230)
    expect(dragWidth(200, -50)).toBe(150)
    expect(dragWidth(200, -500)).toBe(MIN_WIDTH)
  })
})
