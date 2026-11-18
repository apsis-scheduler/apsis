import { columnWidth, dragWidth, loadWidths, MIN_WIDTH, saveWidths } from '@/columns'

describe('column widths', () => {
  test('bad or unavailable storage loads none', () => {
    localStorage.setItem('apsis.runsList.columnWidths', '{')
    expect(loadWidths()).toEqual({})

    const getItem = jest.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('denied') })
    const setItem = jest.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('full') })
    expect(loadWidths()).toEqual({})
    saveWidths({job: 500})
    getItem.mockRestore()
    setItem.mockRestore()
  })

  test('a column takes its saved width else its default', () => {
    expect(columnWidth({job: 500}, 'job')).toBe(500)
    expect(columnWidth({}, 'arg:date')).toBeGreaterThan(0)
  })

  test('dragging moves the right edge by whole pixels down to the minimum', () => {
    expect(dragWidth(200, 30.4)).toBe(230)
    expect(dragWidth(200, -50)).toBe(150)
    expect(dragWidth(200, -500)).toBe(MIN_WIDTH)
  })
})
