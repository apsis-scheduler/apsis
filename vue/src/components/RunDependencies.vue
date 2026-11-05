<template lang="pug">
div.dependencies
  div
  div.l Job
  div.c Run
  div.c State
  div.r Schedule
  div.r Start
  div.r Elapsed
  div.underline1(style="grid-column-end: span 7")

  template(v-if="dependencies !== null")
    div.colhead.v(:style="{ 'grid-row-end': 'span ' + dependencies.reduce((s, d) => s + (d.runs.length || 1) + 1, -1) }") Dependencies
    template(v-for="dep, d of dependencies")
      div.l.col-job.stack(:style="{ 'grid-row-end': 'span ' + dep.runs.length }")
        div: JobWithArgs(:job-id="dep.job_id" :args="dep.args")
        div.omit(v-if="dep.runs.length > MAX_RUNS") last {{ MAX_RUNS }} of {{ dep.runs.length }}
      template(v-for="r, i of dep.runs.slice(-MAX_RUNS)")
        div.c.col-run: Run(:run-id="r.run_id")
        div.c.col-state: State(:state="r.state")
        div.r.col-schedule-time: Timestamp(:time="r.times.schedule")
        div.r.col-start-time: Timestamp(v-if="r.times.running" :time="r.times.running")
        div.r.col-elapsed: RunElapsed(:run="r")
      div(v-if="dep.runs.length == 0")
        div.c.col-run.colspan6 (no runs)
        div
        div
        div
        div
      div.colspan6(v-if="d < dependencies.length - 1" style="border-bottom: 2px dotted #ccc;")
    div.colspan6.v(v-if="dependencies.length == 0") (no dependencies)

  div.underline1(style="grid-column-end: span 7")

  div.colhead.v: span: b This run
  div.l.col-job: JobWithArgs(:job-id="run.job_id" :args="run.args")
  div.c.col-run: Run(:run-id="run.run_id")
  div.c.col-state: State(:state="run.state")
  div.r.col-schedule-time: Timestamp(:time="run.times.schedule")
  div.r.col-start-time: Timestamp(v-if="run.times.running" :time="run.times.running")
  div.r.col-elapsed: RunElapsed(:run="run")

  div.underline1(style="grid-column-end: span 7")

  template(v-if="dependents !== null")
    div.colhead.v(:style="{ 'grid-row-end': 'span ' + dependents.reduce((s, d) => s + Math.max(d.runs.length, 1) + 1, -1) }") Dependents
    template(v-for="dep, d of dependents")
      div.l.col-job.stack(:style="{ 'grid-row-end': 'span ' + dep.runs.length }")
        div: JobWithArgs(:job-id="dep.job_id" :args="dep.args")
        div.omit(v-if="dep.runs.length > MAX_RUNS") last {{ MAX_RUNS }} of {{ dep.runs.length }}
      template(v-for="r, i of dep.runs.slice(-MAX_RUNS)")
        div.c.col-run: Run(:run-id="r.run_id")
        div.c.col-state: State(:state="r.state")
        div.r.col-schedule-time: Timestamp(:time="r.times.schedule")
        div.r.col-start-time: Timestamp(v-if="r.times.running" :time="r.times.running")
        div.r.col-elapsed: RunElapsed(:run="r")
      div(v-if="dep.runs.length == 0")
        div.c.col-run.colspan6 (no runs)
        div
        div
        div
        div
      div.colspan6(v-if="d < dependents.length - 1" style="border-bottom: 2px dotted #ccc;")
    div.colspan6.v(v-if="dependents.length == 0") (no dependents)
</template>

<script>
import JobWithArgs from '@/components/JobWithArgs'
import Run from '@/components/Run'
import { getDependencies, getDependents } from '@/runs'
import RunElapsed from '@/components/RunElapsed'
import State from '@/components/State'
import store from '@/store'
import Timestamp from '@/components/Timestamp'

export default {
  props: ['run'],
  components: {
    JobWithArgs,
    Run,
    RunElapsed,
    State,
    Timestamp,
  },

  data() {
    return {
      // Max number of runs matching job instance to show.
      MAX_RUNS: 8,
    }
  },

  computed: {
    dependencies() {
      return this.run ? getDependencies(this.run, store) : null
    },

    dependents() {
      return this.run ? getDependents(this.run, store) : null
    },
  },
}
</script>

<style lang="scss" scoped>
@import '@/styles/index.scss';

.dependencies {
  display: grid;
  grid-template-columns: max-content minmax(0, max-content) repeat(5, max-content);
  column-gap: 8px;
  row-gap: 1px;
  padding: 16px 0;

  > div {
    margin: 4px 4px;
  }

  .colspan6 {
    grid-column-end: span 6;
  }

  > .l { text-align: left; }
  > .c { text-align: center; }
  > .r { justify-self: end; }

  .v {
    height: 100%;
    display: flex;
    align-items: center;
  }

  .stack {
    display: flex;
    flex-direction: column;
    justify-content: center;
    row-gap: 8px;
  }

  .omit {
    font-size: 85%;
    color: #aaa;
  }

  .colhead {
    margin-top: 0;
    margin-right: 0;
    padding-right: 16px;
    border-right: 2px dotted #ccc;
  }

  .underline1 {
    border-bottom: 2px solid #ccc;
  }

  .col-job {
    min-width: 0;
    overflow-wrap: anywhere;
  }

  .col-run {
  }

  .col-state {
  }

  .col-schedule-time, .col-start-time {
    font-size: 90%;
    color: $global-light-color;
  }

  .col-elapsed {
    white-space: nowrap;
  }
}
</style>
