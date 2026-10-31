import { shallow } from 'vue-test-utils'
import Navbar from '@/components/navbar'

const flush = () => new Promise(resolve => setTimeout(resolve))

describe('navbar', () => {
  test('shows the user from whoami', async () => {
    global.fetch = jest.fn(async () => ({ ok: true, json: async () => ({ user: 'alice' }) }))
    const wrapper = shallow(Navbar, { stubs: ['router-link'] })
    await flush()
    expect(wrapper.text()).toContain('alice')
  })

  test('logs a failed whoami fetch', async () => {
    const err = new Error('down')
    global.fetch = jest.fn(async () => { throw err })
    const log = jest.spyOn(console, 'log').mockImplementation(() => {})
    const wrapper = shallow(Navbar, { stubs: ['router-link'] })
    await flush()
    expect(log).toHaveBeenCalledWith(err)
    expect(wrapper.vm.user).toBe(null)
    log.mockRestore()
  })
})
