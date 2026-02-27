<template lang="pug">
div.component
  div
    span.title
      | Job 
      Job(:job-id="job_id")
    span(v-if="job && job.metadata.labels")
      JobLabel.label(v-for="label in job.metadata.labels" :key="label" :label="label")

  div(v-if="job" style="font-size:120%;")
    | Parameters: ({{ params }})

  div.error-message(v-if="job === null") This job does not currently exist.  Past runs may be shown.

  Frame(v-if="job && job.metadata.description" title="Description")
    div(v-html="markdown(job.metadata.description)")

  Frame(title="Runs")
    RunsList(
      :query="{job_id: job_id, show: 20}" 
      :show-job="false"
      :job-controls="false"
      argColumnStyle="separate"
      style="max-height: 60em; overflow-y: auto;"
    )

  Frame(title="Details")
    table.fields(v-if="job"): tbody
      tr
        th program
        td.no-padding: Program(:program="job.program")

      //- Schedules with inline dependencies.
      tr
        th schedules
        td(v-if="job.schedule.length > 0")
          .schedule-group(v-for="(group, gi) in scheduleGroups" :key="'sg-' + gi")
            .schedule-header
              span.schedule-args-header(v-if="Object.keys(group.args).length > 0")
                | (
                RunArgs(:args="group.args")
                | )
              span(
                v-for="sched in group.schedules"
                :key="sched.str"
                :class="{ disabled: !sched.enabled }"
              ) &nbsp;{{ stripSchedArgs(sched.str) }}{{ sched.enabled ? '' : ' (disabled)' }}
            .schedule-deps
              .dep-line(v-if="hasCommonDeps && scheduleGroups.length > 1")
                span.dep-chrome Depends on all common dependencies (
                a.dep-common-link(href="#common-deps") see below
                span.dep-chrome )
              .dep-line(v-for="(cond, ci) in group.conditions" :key="'dep-' + gi + '-' + ci")
                template(v-if="cond.type === 'dependency'")
                  span.dep-chrome Depends on {{ join(cond.states || ['success'], '|') }} of:&nbsp;
                  Job(v-if="cond.resolved_job_id" :job-id="cond.resolved_job_id")
                  span.dep-job-id(v-else) {{ cond.job_id }}
                  span.dep-args(v-if="cond.resolved_args && Object.keys(cond.resolved_args).length > 0")
                    |  (
                    RunArgs(:args="cond.resolved_args")
                    | )
                span.dep-chrome(v-else) {{ cond.str }}
              template(v-if="scheduleGroups.length <= 1")
                .dep-line(v-for="cond in job.common_conditions" :key="'inline-' + cond.str")
                  template(v-if="cond.type === 'dependency'")
                    span.dep-chrome Depends on {{ join(cond.states || ['success'], '|') }} of:&nbsp;
                    Job(:job-id="cond.job_id")
                    span.dep-args(v-if="cond.resolved_args && Object.keys(cond.resolved_args).length > 0")
                      |  (
                      RunArgs(:args="cond.resolved_args")
                      | )
                  span.dep-chrome(v-else) {{ cond.str }}
          .schedule-group#common-deps(v-if="hasCommonDeps && scheduleGroups.length > 1")
            .schedule-header
              span.schedule-all All Schedules
            .schedule-deps
              .dep-line(v-for="cond in job.common_conditions" :key="'common-' + cond.str")
                template(v-if="cond.type === 'dependency'")
                  span.dep-chrome Depends on {{ join(cond.states || ['success'], '|') }} of:&nbsp;
                  Job(:job-id="cond.job_id")
                  span.dep-args(v-if="cond.resolved_args && Object.keys(cond.resolved_args).length > 0")
                    |  (
                    RunArgs(:args="cond.resolved_args")
                    | )
                span.dep-chrome(v-else) {{ cond.str }}
        td(v-else) No schedules.

      //- Condition definitions.
      tr
        th conditions
        td(v-if="job.condition.length > 0")
          details.condition-details
            summary.condition-summary Definitions (click to expand)
            ul.condition-list
              li(v-for="cond in job.condition" :key="'def-' + cond.str")
                template(v-if="cond.type === 'dependency'")
                  span
                    span.dep-chrome Depends on {{ join(cond.states || ['success'], '|') }} of&nbsp;
                    span.dep-job-id {{ cond.job_id }}
                span.dep-chrome(v-else) {{ cond.str }}
                ul.enable-if-list(v-if="cond.enabled != null")
                  li
                    span.enable-if enabled: {{ cond.enabled }}
        td(v-else) No conditions.

      tr
        th actions
        td.no-padding(v-if="job.action.length > 0")
          .action(v-for="action in job.action"): table.fields
            tr(v-for="(value, key, i) in action" :key="i")
              th {{ key }}
              td {{ value }}
        td(v-else) No actions.

      tr(v-if="Object.keys(metadata).length")
        th metadata
        td.no-padding: table.fields
          tr(
            v-for="(value, key) in metadata" 
            :key="key"
          )
            th {{ key }}
            td {{ value }}

  Frame(title="Schedule Run")
    table.schedule(v-if="job")
      tr(v-for="param in (job.params || [])" :key="'schedule' + param")
        th {{ param }}:
        td
          input(
            @input="setScheduleArg(param, $event)"
            @focus="scheduledRunId = ''"
          )
      tr.time
       th Schedule Time:
       td
        input(
          v-model="scheduleTime"
          placeholder="time"
        )
      tr.submit
        td
        td
          button(
            :disabled="! scheduleReady"
            @click="scheduleRun"
            :title="!scheduleReady ? 'Please fill in required arguments' : null"
          ) Schedule
          span(v-if="scheduledRunId") Scheduled: &nbsp;
          Run(v-if="scheduledRunId" :runId="scheduledRunId")

</template>

<script>
import * as api from '@/api'
import { every, isEqual, join, pickBy } from 'lodash'
import ConfirmationModal from '@/components/ConfirmationModal'
import Frame from '@/components/Frame'
import Job from '@/components/Job'
import JobLabel from '@/components/JobLabel'
import Program from '@/components/Program'
import Run from '@/components/Run'
import RunArgs from '@/components/RunArgs'
import RunsList from '@/components/RunsList'
import showdown from 'showdown'
import store from '@/store'
import { formatTime, parseTime } from '@/time'
import Vue from 'vue'

export default {
  props: ['job_id'],

  components: {
    Frame,
    Job,
    JobLabel,
    Program,
    Run,
    RunArgs,
    RunsList,
  },

  data() {
    return {
      // Form fields for schedule pane.
      scheduleArgs: {},
      scheduleTime: 'now',
      scheduledInstance: null,
      scheduledRunId: null,
    }
  },

  computed: {
    job() {
      return store.state.jobs.get(this.job_id)
    },

    params() {
      return this.job.params ? join(this.job.params, ', ') : []
    },

    // Metadata filtered, with keys omitted that are displayed specially.
    metadata() {
      return pickBy(
        this.job.metadata,
        (v, k) => k !== 'description' && k !== 'labels'
      )
    },

    hasCommonDeps() {
      return this.job && this.job.common_conditions && this.job.common_conditions.length > 0
    },

    scheduleGroups() {
      if (!this.job || this.job.schedule.length === 0) return []
      const groups = []
      const seen = []
      for (const sched of this.job.schedule) {
        const args = sched.args || {}
        if (seen.some(s => isEqual(s, args))) continue
        seen.push(args)
        const schedules = this.job.schedule.filter(s => isEqual(s.args || {}, args))
        const resolved = (this.job.resolved_conditions || []).find(
          g => isEqual(g.schedule_args, args)
        )
        groups.push({
          args,
          schedules,
          conditions: resolved ? resolved.conditions : []
        })
      }
      return groups
    },

    scheduleReady() {
      return (
        every(this.job.params.map(p => this.scheduleArgs[p]))
        && (this.scheduleTime === 'now'
          || parseTime(this.scheduleTime, false, store.state.timeZone)
        )
      )
    },
  },

  methods: {
    markdown(src) { return src.trim() === '' ? '' : (new showdown.Converter()).makeHtml(src) },

    stripSchedArgs(str) { return str.replace(/^\([^)]*\)\s*/, '') },

    setScheduleArg(param, ev) {
      this.$set(this.scheduleArgs, param, ev.target.value)
      this.scheduledRun = null
    },

    scheduleRun() {
      const tz = store.state.timeZone
      const time = (
        this.scheduleTime === 'now' ? 'now' 
        : parseTime(this.scheduleTime, false, tz)
      )
      console.assert(time, 'can\'t parse time')

      const url = api.getSubmitRunUrl()
      const body = api.getSubmitRunBody(this.job_id, this.scheduleArgs, time === 'now' ? 'now' : time.format())

      const instance = (
        this.job.job_id + ' (' 
        + join(this.job.params.map(p => p + '=' + this.scheduleArgs[p]), ' ') + ')'
      )
      const message = (
        'Schedule ' + instance + ' for ' 
        + (time === 'now' ? 'now' : formatTime(time, tz) + ' ' + tz) + '?'
      )

      const fn = () => 
        fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(body)
        })
          .then(async (response) => {
            if (response.ok) {
              const result = await response.json()
              this.scheduledRunId = Object.keys(result.runs)[0]
            }
          })

      const Class = Vue.extend(ConfirmationModal)
      const modal = new Class({propsData: {message, ok: fn}})
      // Mount and add the modal.  The modal destroys and removes itself.
      modal.$mount()
      this.$root.$el.appendChild(modal.$el)
    },

    join,
  },
}
</script>

<style lang="scss" scoped>
@import '../styles/vars.scss';

.component > div {
  margin-bottom: 1rem;
}

.title {
  margin-right: 1ex;
}

.label {
  position: relative;
  top: -4px;
}

// This is rubbish.
.action {
  padding-top: 8px;
  th, td {
    line-height: 1;
  }
}

.disabled {
  color: $global-light-color;
}

.condition-section {
  &:first-child {
    margin-top: 0.5em;
  }
  &:not(:first-child) {
    margin-top: 0.75em;
  }
}

.section-label {
  font-size: 0.85em;
  font-weight: bold;
  text-transform: uppercase;
  color: #888;
}

.condition-details {
  summary {
    cursor: pointer;
    color: #888;
    font-size: 0.9em;
    &:hover {
      color: #555;
    }
  }
}

.condition-list {
  margin: 0.15em 0 0 0;
  padding-left: 1.5em;
}

.dep-chrome {
  color: #999;
  font-size: 0.9em;
}

.dep-job-id {
  font-weight: 500;
}

.dep-args {
  color: #666;
  margin: 0 0.25em;
}

.schedule-group {

  list-style: disc;
  display: list-item;
  margin-left: 1.5em;
  &:not(:first-child) {
    margin-top: 0.75em;
  }
}

.schedule-args-header {
  color: #676;
  font-weight: 600;
}

.schedule-all {
  font-size: 0.85em;
  font-weight: bold;
  text-transform: uppercase;
  color: #888;
}

.schedule-deps {
  margin-left: 1.5em;
  margin-top: 0.15em;
}

.dep-line {
  margin: 0.15em 0;
}

.dep-common-link {
  color: #999;
  font-size: 0.9em;
  text-decoration: underline;
}

.enable-if {
  color: #888;
  font-style: italic;
}

.enable-if-list {
  margin: 0.1em 0 0 0;
  padding-left: 1.5em;
}


.schedule {
  border-spacing: 8px 4px;
  white-space: nowrap;

  th {
    text-align: right;
    text-transform: uppercase;
    font-weight: normal;
    font-size: 0.875rem;
    color: #999;
  }

  td {
    height: 38px;
  }

  input {
    width: 24em;
  }

  button {
    background: #f0f6f0;
    margin-right: 24px;
    padding: 0 20px;
    border-radius: 3px;
    border: 1px solid #aaa;
    font-size: 85%;
    text-transform: uppercase;
    white-space: nowrap;
    cursor: default;

    &:hover:not(:disabled) {
      background: #90e0a0;
    }

    &:disabled {
      background: #e0e0e0;
      color: #888;
      border-color: #ccc;
      cursor: not-allowed;
      opacity: 0.6;
    }
  }

  .time {
    td, th {
      padding-top: 20px;
    }
  }

  .submit {
    td {
      padding-top: 20px;
    }
    td:first-child {
      text-align: right;
    }
  }
}
 </style>
