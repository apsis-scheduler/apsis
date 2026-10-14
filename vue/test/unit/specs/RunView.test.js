import Vue from 'vue'
import RunView from '@/views/RunView'
import store from '@/store.js'

jest.mock('@/websocket.js', () => ({
  Socket: class { open() {} close() {} },
}))

Vue.component('router-link', { render: h => h('a') })
Vue.prototype.$route = { query: {} }

describe('RunView.vue', () => {
  it('shows the run and job in the title', async () => {
    global.fetch = jest.fn(() => new Promise(() => {}))
    store.state.runs = new Map([['r1', {
      run_id: 'r1',
      job_id: 'test/job',
      args: {},
      state: 'success',
      times: {},
    }]])

    const vm = new (Vue.extend(RunView))({
      propsData: { run_id: 'r1' },
    }).$mount()
    await vm.$nextTick()
    expect(document.title).toEqual('r1 - test/job - apsis')
  })
})
