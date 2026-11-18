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

  it('shows labels on their own line under the title', () => {
    const job = {
      job_id: 'test/job',
      params: [],
      metadata: {labels: ['no-ops-board', 'stage']},
      schedule: [],
      condition: [],
      action: [],
      program: {str: 'true'},
    }
    store.state.jobs = new Map([['test/job', job]])
    const vm = new (Vue.extend(JobView))({ propsData: { job_id: 'test/job' } }).$mount()
    const labels = vm.$el.querySelector('.job-labels')
    expect(Array.from(labels.querySelectorAll('.job-label'), e => e.textContent.trim()))
      .toEqual(['no-ops-board', 'stage'])
    expect(vm.$el.querySelector('.title').contains(labels)).toBe(false)
  })
})
