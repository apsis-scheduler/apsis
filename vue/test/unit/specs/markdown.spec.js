import showdown from 'showdown'
import { descriptionExcerptHtml } from '@/markdown'

jest.mock('showdown', () => {
  const real = new (jest.requireActual('showdown').Converter)()
  const converter = { makeHtml: jest.fn(src => real.makeHtml(src)) }
  return { Converter: function () { return converter } }
})

describe('descriptionExcerptHtml', () => {
  it('renders only the first paragraph, truncated', () => {
    expect(descriptionExcerptHtml('*first*\n\nsecond')).toEqual('<p><em>first</em></p>')
    expect(descriptionExcerptHtml('x'.repeat(300))).toEqual('<p>' + 'x'.repeat(256) + '…</p>')
    expect(descriptionExcerptHtml('  \n\nsecond')).toEqual('')
  })

  it('renders each description once', () => {
    const { makeHtml } = new showdown.Converter()
    makeHtml.mockClear()
    expect(descriptionExcerptHtml('cached')).toEqual('<p>cached</p>')
    expect(descriptionExcerptHtml('cached')).toEqual('<p>cached</p>')
    expect(makeHtml).toHaveBeenCalledTimes(1)
  })
})
