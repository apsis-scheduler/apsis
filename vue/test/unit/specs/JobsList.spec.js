import Vue from 'vue'
import JobsList from '@/components/JobsList'
import store from '@/store'

// jsdom has no ResizeObserver so tests run the body observer by hand
let bodyObserver = null
window.ResizeObserver = class {
  constructor(callback) { this.callback = callback }
  observe(target) { if (target === document.body) bodyObserver = this }
  unobserve() {}
  disconnect() { if (bodyObserver === this) bodyObserver = null }
}

function makeJob(job_id) {
  return { job_id, params: [], metadata: { labels: [], description: '' } }
}

describe('JobsList.vue', () => {
  it('filters by keywords while typing and emits them on commit', async () => {
    store.state.jobs = new Map(['alpha/one', 'beta/two'].map(id => [id, makeJob(id)]))
    const vm = new (Vue.extend(JobsList))({ propsData: { keywords: null } }).$mount()
    const emitted = []
    vm.$on('keywords', keywords => emitted.push(keywords))
    const input = vm.$el.querySelectorAll('.controls input')[0]
    const rowNames = () => vm.jobRows.map(({ name }) => name)
    expect(rowNames()).toEqual(['alpha', 'beta'])

    input.value = 'one'
    input.dispatchEvent(new Event('input'))
    await Vue.nextTick()
    expect(rowNames()).toEqual(['alpha'])
    expect(emitted).toEqual([])

    input.dispatchEvent(new Event('change'))
    expect(emitted).toEqual([['one']])

    input.value = ''
    input.dispatchEvent(new Event('input'))
    await Vue.nextTick()
    expect(rowNames()).toEqual(['alpha', 'beta'])
    // cleared text matches the null keywords prop so there's nothing to emit
    input.dispatchEvent(new Event('change'))
    expect(emitted).toEqual([['one']])
  })

  it('keys a dir apart from a job with the same name', () => {
    store.state.jobs = new Map(['a', 'a/b'].map(id => [id, makeJob(id)]))
    const vm = new (Vue.extend(JobsList))().$mount()
    expect(vm.jobRows.map(({ key }) => key)).toEqual(['a/', 'a'])
  })

  it('remeasures the scroller when anything above it resizes', () => {
    store.state.jobs = new Map()
    const vm = new (Vue.extend(JobsList))().$mount()
    vm.$refs.scroller.$el.getBoundingClientRect = () => ({top: 300})
    bodyObserver.callback()
    expect(vm.scrollerTop).toBe(300)

    vm.$destroy()
    expect(bodyObserver).toBe(null)
  })
})
