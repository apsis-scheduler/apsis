import Vue from 'vue'
import VueRouter from 'vue-router'
import JobsView from '@/views/JobsView'
import store from '@/store'

Vue.use(VueRouter)

function makeJob(job_id, labels) {
  return { job_id, params: [], metadata: { labels, description: '' } }
}

describe('JobsView.vue', () => {
  it('toggles a label filter in the url when a label is clicked', async () => {
    store.state.jobs = new Map([
      makeJob('one', ['critical', 'daily']),
      makeJob('two', ['daily']),
    ].map(job => [job.job_id, job]))
    const router = new VueRouter({
      mode: 'abstract',
      routes: [{ path: '/jobs/:path*', name: 'jobs-list', component: JobsView }],
    })
    router.push('/jobs?labels=daily')
    const vm = new Vue({ router, render: h => h('router-view') }).$mount()
    await Vue.nextTick()
    const rowNames = () => vm.$children[0].$children[0].jobRows.map(({ name }) => name)
    const label = (job, label) => Array.from(vm.$el.querySelectorAll('.row'))
      .find(row => row.querySelector('.name').textContent === job)
      .querySelector(`.job-label:nth-child(${label})`)
    expect(rowNames()).toEqual(['one', 'two'])
    expect(label('one', 2).classList).toContain('selected')

    label('one', 1).click()
    await Vue.nextTick()
    expect(router.currentRoute.query.labels).toEqual('daily,critical')
    expect(rowNames()).toEqual(['one'])
    expect(label('one', 1).classList).toContain('selected')

    label('one', 1).click()
    await Vue.nextTick()
    expect(router.currentRoute.query.labels).toEqual('daily')
    expect(rowNames()).toEqual(['one', 'two'])
    expect(label('one', 1).classList).not.toContain('selected')

    label('two', 1).click()
    await Vue.nextTick()
    expect(router.currentRoute.query.labels).toBe(undefined)
  })
})
