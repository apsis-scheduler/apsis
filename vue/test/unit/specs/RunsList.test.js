import Vue from 'vue'
import RunsList from '@/components/RunsList'
import store from '@/store.js'

Vue.component('router-link', { render: h => h('a') })

function mountRunsList(propsData) {
  store.state.runs = new Map([['r1', {
    run_id: 'r1',
    job_id: 'test/job',
    args: {fruit: 'mango'},
    labels: ['daily'],
    state: 'success',
    times: {schedule: '2026-10-14T12:00:00Z'},
    time_key: '2026-10-14T12:00:00Z',
    group_key: 'C test/job',
  }]])
  return new (Vue.extend(RunsList))({ propsData }).$mount()
}

describe('RunsList.vue', () => {
  it('toggles a label filter when a label is clicked', async () => {
    const vm = mountRunsList({})
    const queries = []
    vm.$on('query', query => queries.push(query))

    vm.$el.querySelector('.job-label').click()
    await vm.$nextTick()
    expect(queries.pop().labels).toEqual(['daily'])
    expect(vm.$el.querySelector('.job-label.selected')).not.toBe(null)

    vm.$el.querySelector('.job-label').click()
    await vm.$nextTick()
    expect(queries.pop().labels).toBe(null)
  })

  it('adds an arg filter when an arg is clicked', async () => {
    for (const argColumnStyle of ['combined', 'separate']) {
      const vm = mountRunsList({argColumnStyle})
      vm.$el.querySelector('.col-args .run-arg, .arg-value').click()
      await vm.$nextTick()
      expect(vm.args).toEqual({fruit: 'mango'})
    }
  })

  it('ignores clicks and highlights nothing when the filter controls are hidden', async () => {
    for (const argColumnStyle of ['combined', 'separate']) {
      const query = {args: {fruit: 'mango'}, labels: ['daily']}
      const vm = mountRunsList({argColumnStyle, query, jobControls: false, runControls: false})
      vm.$el.querySelector('.job-label').click()
      vm.$el.querySelector('.col-args .run-arg, .arg-value').click()
      await vm.$nextTick()
      expect(vm.labels).toEqual(['daily'])
      expect(vm.args).toEqual({fruit: 'mango'})
      expect(vm.$el.querySelector('.job-label.selected, .run-arg.selected, .arg-value.selected')).toBe(null)
    }
  })

  it('resizes a column by dragging its header edge and resets it on double click', async () => {
    const saved = new Map()
    Object.defineProperty(window, 'localStorage', {
      value: {getItem: key => saved.get(key) || null, setItem: (key, value) => saved.set(key, value)},
      configurable: true,
    })
    const vm = mountRunsList({})
    const col = () => vm.$el.querySelector('col').style.width
    const handle = vm.$el.querySelector('th.col-job .col-resizer')
    const start = col()

    // a click without a drag saves nothing
    handle.dispatchEvent(new MouseEvent('mousedown', {clientX: 100}))
    window.dispatchEvent(new MouseEvent('mouseup'))
    expect(saved.size).toBe(0)

    handle.dispatchEvent(new MouseEvent('mousedown', {clientX: 100}))
    window.dispatchEvent(new MouseEvent('mousemove', {clientX: 160}))
    window.dispatchEvent(new MouseEvent('mouseup'))
    await vm.$nextTick()
    expect(col()).toBe(parseInt(start) + 60 + 'px')
    expect(mountRunsList({}).$el.querySelector('col').style.width).toBe(col())

    handle.dispatchEvent(new MouseEvent('dblclick'))
    await vm.$nextTick()
    expect(col()).toBe(start)
    expect(mountRunsList({}).$el.querySelector('col').style.width).toBe(start)
    delete window.localStorage
  })
})
