import Vue from 'vue'
import RunView from '@/views/RunView'
import store from '@/store.js'

jest.mock('@/websocket.js', () => ({
  Socket: class { open() {} close() {} },
}))

Vue.component('router-link', { render: h => h('a') })
Vue.prototype.$route = { query: {} }

describe('RunView.vue', () => {
  it('shows the run and job in the title and the run state as the favicon', async () => {
    document.head.innerHTML = '<link rel="icon" href="/static/favicon.svg">'
    const favicon = () => document.querySelector('link[rel="icon"]').getAttribute('href')
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
    const icon = decodeURIComponent(favicon())
    expect(icon).toContain('xmlns="http://www.w3.org/2000/svg"')
    expect(icon).toContain('fill="#40a060"')

    vm.$destroy()
    expect(favicon()).toEqual('/static/favicon.svg')
  })

  it('resets the favicon when the run is missing', async () => {
    document.head.innerHTML = '<link rel="icon" href="/static/favicon.svg">'
    const favicon = () => document.querySelector('link[rel="icon"]').getAttribute('href')
    let respond
    global.fetch = jest.fn(() => new Promise(resolve => { respond = resolve }))
    store.state.runs = new Map([['r1', {
      run_id: 'r1',
      job_id: 'test/job',
      args: {},
      state: 'new',
      times: {},
    }]])

    const vm = new (Vue.extend(RunView))({
      propsData: { run_id: 'r1' },
    }).$mount()
    await vm.$nextTick()
    expect(favicon()).not.toEqual('/static/favicon.svg')

    respond({ok: false, status: 404})
    await new Promise(resolve => setTimeout(resolve))
    expect(favicon()).toEqual('/static/favicon.svg')
  })
})
