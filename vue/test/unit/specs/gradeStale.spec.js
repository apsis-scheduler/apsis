import Vue from 'vue'
import router from '@/router'
import store from '@/store.js'

jest.mock('@/websocket.js', () => {
  const sockets = []
  class Socket {
    constructor(url, onMessage, onConnect, onErr, onClose) {
      Object.assign(this, {url, onMessage, onConnect, onErr, onClose})
      this.closed = false
      sockets.push(this)
    }
    open() {}
    close() { this.closed = true }
  }
  return {Socket, sockets}
})
const { sockets } = require('@/websocket.js')

const flush = () => new Promise(resolve => setTimeout(resolve))
const frame = (...msgs) => ({data: JSON.stringify(msgs)})

test('switching from an old past window to now opens one socket', async () => {
  global.fetch = jest.fn(() => new Promise(() => {}))
  const vm = new Vue({router, render: h => h('router-view')}).$mount()
  await router.push('/runs?time=20261014T100000Z')
  await flush()
  sockets[sockets.length - 1].onMessage(frame({type: 'run_window', anchor: '2026-10-14T10:00:00+00:00', groups: {},
    earlier: null, later: {time: '2026-10-14T10:10:00+00:00'}}))
  await flush()
  expect(store.state.runWindow).not.toBe(null)
  // the window was loaded more than five minutes ago
  store.state.time = new Date(Date.now() + 10 * 60 * 1000)
  await flush()
  const n = sockets.length
  await router.push('/runs')
  await flush()
  console.log(sockets.map(s => [s.url.search, s.closed]))
  expect(sockets.length).toBe(n + 1)
  vm.$destroy()
})
