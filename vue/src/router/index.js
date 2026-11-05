import Vue from 'vue'
import Router from 'vue-router'

import { setTitle } from '@/page'

import AgentsView   from '@/views/AgentsView'
import JobView      from '@/views/JobView'
import JobsView     from '@/views/JobsView'
import RunView      from '@/views/RunView'
import RunsView     from '@/views/RunsView'

Vue.use(Router)

const router = new Router({
  mode: 'history',
  routes: [
    {
      path: '/',
      redirect: '/jobs',
    },
    {
      path: '/agents',
      props: false,
      name: 'agents',
      meta: { title: 'agents' },
      component: AgentsView,
    },
    {
      path: '/jobs/:path*',
      props: true,
      name: 'jobs-list',
      meta: { title: 'jobs' },
      component: JobsView,
    },
    {
      path: '/job/:job_id*',
      props: true,
      name: 'job',
      component: JobView,
    },
    {
      path: '/runs/:run_id',
      props: true,
      name: 'run',
      component: RunView,
    },
    {
      path: '/runs',
      name: 'runs-list',
      meta: { title: 'runs' },
      component: RunsView,
    },
  ]
})

// job and run views set their own titles
router.afterEach(to => {
  if (to.meta.title)
    setTitle(to.meta.title)
})

export default router
