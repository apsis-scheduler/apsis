import Vue from 'vue'
import JobView from '@/views/JobView'
import store from '@/store.js'

Vue.component('router-link', { render: h => h('a') })

describe('JobView.vue', () => {
  it('shows the job id in the title', () => {
    store.state.jobs = new Map()
    new (Vue.extend(JobView))({ propsData: { job_id: 'test/job' } }).$mount()
    expect(document.title).toEqual('test/job - apsis')
  })
})
