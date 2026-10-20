import { setTitle } from '@/page'

describe('setTitle', () => {
  test('puts the most specific part first and skips missing parts', () => {
    setTitle('r1234', 'test/job')
    expect(document.title).toEqual('r1234 - test/job - apsis')
    setTitle('r1234', null)
    expect(document.title).toEqual('r1234 - apsis')
  })
})
