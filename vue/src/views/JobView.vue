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
          .schedule-entry(
            v-for="(entry, i) in scheduleEntries"
            :key="'se-' + i"
            :id="entry.isCommon ? 'common-deps' : null"
          )
            .schedule-header
              span.schedule-all(v-if="entry.isCommon") Common Conditions
              template(v-else)
                span.schedule-args-header(v-if="Object.keys(entry.args).length > 0")
                  | (
                  RunArgs(:args="entry.args")
                  | )&nbsp;
                span(:class="{ disabled: !entry.schedule.enabled }")
                  | {{ stripSchedArgs(entry.schedule.str) }}{{ entry.schedule.enabled ? '' : ' (disabled)' }}
            .schedule-deps
              .dep-line(v-if="entry.showCommonLink")
                span.dep-chrome depends on all common conditions (
                a.dep-common-link(href="#common-deps") see below
                span.dep-chrome )
              .dep-line(v-for="(cond, ci) in entry.conditions" :key="'dep-' + i + '-' + ci")
                template(v-if="cond.type === 'dependency'")
                  span.dep-chrome depends on {{ join(cond.states || ['success'], '|') }} of:&nbsp;
                  Job(v-if="cond.resolved_job_id" :job-id="cond.resolved_job_id")
                  span(v-else) {{ cond.job_id }}
                  span.dep-args(v-if="cond.resolved_args && Object.keys(cond.resolved_args).length > 0")
                    |  (
                    RunArgs(:args="cond.resolved_args")
                    | )
                span.dep-chrome(v-else) {{ cond.str }}
        td(v-else) No schedules.

      tr
        th conditions
        td(v-if="job.condition.length > 0")
          details.condition-details
            summary Definitions
            ul.condition-list
              li(v-for="cond in job.condition" :key="'def-' + cond.str")
                template(v-if="cond.type === 'dependency'")
                  span
                    span.dep-chrome depends on {{ join(cond.states || ['success'], '|') }} of&nbsp;
                    span {{ cond.job_id }}
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

    scheduleEntries() {
      if (!this.job || this.job.schedule.length === 0) return []
      const resolved = this.job.resolved_conditions || []
      const common = this.job.common_conditions || []

      // Check if there are multiple unique arg sets.
      const argSets = []
      for (const sched of this.job.schedule) {
        const args = sched.args || {}
        if (!argSets.some(s => isEqual(s, args)))
          argSets.push(args)
      }
      const hasCommon = argSets.length > 1 && common.length > 0

      const entries = this.job.schedule.map(sched => {
        const args = sched.args || {}
        const r = resolved.find(g => isEqual(g.schedule_args, args))
        return {
          schedule: sched,
          args,
          showCommonLink: hasCommon,
          conditions: [
            ...(r ? r.conditions : []),
            ...(!hasCommon ? common : []),
          ],
        }
      })

      if (hasCommon)
        entries.push({ schedule: null, args: {}, conditions: common, isCommon: true })

      return entries
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

table.fields td {
  font-family: "Roboto Mono", monospace;
}

.condition-details {
  summary {
    cursor: pointer;
    font-size: 0.85em;
    font-weight: bold;
    text-transform: uppercase;
    color: #888;
    &:hover {
      color: #555;
    }
  }
}

.condition-list {
  margin: 0.15em 0 0 0;
  padding-left: 3em;
  list-style: none;
}

.dep-chrome {
  color: #999;
  font-size: 0.9em;
}

.dep-args {
  color: #666;
  margin: 0 0.25em;
}

.schedule-entry {
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
  color: $apsis-job-color;
  font-size: 0.9em;
  cursor: default;
  &:hover {
    color: $apsis-job-color;
    text-decoration: underline;
  }
}

.enable-if {
  color: #888;
  font-style: italic;
}

.enable-if-list {
  margin: 0.1em 0 0 0;
  padding-left: 1.5em;
  list-style: none;
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
