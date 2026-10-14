import Vue from 'vue'
import JobsList from '@/components/JobsList'
import store from '@/store'

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
  })
})
