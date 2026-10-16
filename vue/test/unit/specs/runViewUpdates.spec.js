import Vue from 'vue'
import RunView from '@/views/RunView'
import store from '@/store.js'
import { sockets } from '@/websocket.js'

jest.mock('@/websocket.js', () => {
  const sockets = []
  class Socket {
    constructor(url, onMessage) {
      Object.assign(this, {url, onMessage})
      sockets.push(this)
    }
    open() {}
    close() {}
  }
  return {Socket, sockets}
})

Vue.component('router-link', { render: h => h('a') })
Vue.prototype.$route = { query: {} }

const flush = () => new Promise(resolve => setTimeout(resolve))

const SUMMARY = {run_id: 'r1', job_id: 'test/job', args: {}, state: 'scheduled', times: {}}
const FULL = {...SUMMARY, state: 'new', program: {str: 'echo'}, meta: {}}

let fetches

// resolves the pending fetch of run `run_id`
function respond(run_id, response) {
  fetches.find(f => f.url.endsWith('/runs/' + run_id)).resolve(response)
  return flush()
}

const ok = run => ({ok: true, json: async () => ({runs: {[run.run_id]: run}})})

function sendSummary(summary) {
  sockets[sockets.length - 1].onMessage({data: JSON.stringify({run: summary})})
}

function mount(run_id) {
  const parent = new Vue({
    data: {run_id},
    render(h) { return h(RunView, {props: {run_id: this.run_id}, ref: 'view'}) },
  }).$mount()
  return [parent, parent.$refs.view]
}

beforeEach(() => {
  store.state.runs = new Map()
  fetches = []
  global.fetch = jest.fn(url => new Promise(resolve => fetches.push({url: url.toString(), resolve})))
})

describe('RunView run updates', () => {
  test('a summary updates the run without refetching', async () => {
    const [, vm] = mount('r1')
    await respond('r1', ok(FULL))
    sendSummary(SUMMARY)
    expect(vm.run.state).toBe('scheduled')
    expect(vm.run.program).toEqual(FULL.program)
    expect(fetch).toHaveBeenCalledTimes(1)

    // other messages leave the run alone
    const run = vm.run
    sockets[sockets.length - 1].onMessage({data: JSON.stringify({meta: {elapsed: 1}})})
    expect(vm.meta).toEqual({elapsed: 1})
    expect(vm.run).toBe(run)
  })

  test('a summary before the fetch response is applied to it', async () => {
    const [, vm] = mount('r1')
    sendSummary(SUMMARY)
    expect(vm.run).toBe(null)
    await respond('r1', ok(FULL))
    expect(vm.run.state).toBe('scheduled')
    expect(vm.run.program).toEqual(FULL.program)
  })

  test('a response for a previous run is dropped', async () => {
    const [parent, vm] = mount('r1')
    parent.run_id = 'r2'
    await flush()
    await respond('r1', ok(FULL))
    expect(vm.run).toBe(null)
  })

  test('a failed fetch shows an error', async () => {
    mount('r1')
    await respond('r1', {ok: false, status: 500, text: async () => 'boom'})
    expect(store.state.errors.errors.map(e => e.message))
      .toEqual([expect.stringMatching(/ 500 boom$/)])
  })
})
