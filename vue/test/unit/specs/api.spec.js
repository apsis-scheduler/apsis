import * as api from '@/api'

describe('Urls', () => {
  test('mark URL matches', () => {
    expect(api.getMarkUrl('r1234', 'success').href).toEqual('http://localhost/api/v1/runs/r1234/mark/success')
    expect(api.getMarkUrl('r567', 'failure').href).toEqual('http://localhost/api/v1/runs/r567/mark/failure')
  })

  test('output data URL matches', () => {
    expect(api.getOutputDataUrl('r1234', 'output').href).toEqual('http://localhost/api/v1/runs/r1234/output/output')
    expect(api.getOutputDataUrl('r1234', 'log').href).toEqual('http://localhost/api/v1/runs/r1234/output/log')
  })

  test('output URL matches', () => {
    expect(api.getOutputUrl('r1234').href).toEqual('http://localhost/api/v1/runs/r1234/outputs')
  })

  test('rerun URL matches', () => {
    expect(api.getRerunUrl('r1234').href).toEqual('http://localhost/api/v1/runs/r1234/rerun')
  })

  test('run URL matches', () => {
    expect(api.getRunUrl('r1234').href).toEqual('http://localhost/api/v1/runs/r1234')
  })
})
