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

      tr
        th schedule
        td(v-if="job.schedule.length > 0")
          li(
            v-for="schedule in job.schedule"
            :key="schedule.str"
            :class="{ disabled: !schedule.enabled }"
          ) {{ schedule.str }} {{ schedule.enabled ? '' : '(disabled)' }}
        td(v-else) No schedules.

      tr
        th conditions
        td(v-if="hasConditions")
          .condition-section(v-if="hasParameterized")
            .section-label Definitions
            ul.condition-list
              li(v-for="cond in job.condition" :key="'def-' + cond.str")
                template(v-if="cond.type === 'dependency'")
                  span
                    span.dep-chrome dependency&nbsp;
                    span.dep-job-id {{ cond.job_id }}
                    span.dep-chrome  is {{ join(cond.states || ['success'], '|') }}
                  ul.enable-if-list(v-if="cond.enabled != null")
                    li
                      span.enable-if enabled: {{ cond.enabled }}
                span(v-else) {{ cond.str }}
          .condition-section(v-if="job.common_conditions && job.common_conditions.length > 0")
            .section-label(v-if="hasParameterized") Dependencies Common to All Arg Sets
            ul.condition-list
              li(v-for="cond in job.common_conditions" :key="cond.str")
                span(v-if="cond.type === 'dependency'")
                  | dependency:&nbsp;
                  Job(:job-id="cond.job_id")
                  span.dep-args(v-if="cond.resolved_args && Object.keys(cond.resolved_args).length > 0")
                    |  (
                    RunArgs(:args="cond.resolved_args")
                    | )
                  |  is {{ join(cond.states || ['success'], '|') }}
                span(v-else) {{ cond.str }}
          .condition-section(v-if="hasParameterized")
            .section-label Dependencies (by scheduled arg sets)
            ul.condition-list
              li(v-for="(group, gi) in job.resolved_conditions" :key="'group-' + gi")
                span.schedule-args
                  | (
                  RunArgs(:args="group.schedule_args")
                  | )
                ul
                  li(v-for="(cond, ci) in group.conditions" :key="'resolved-' + gi + '-' + ci")
                    span(v-if="cond.type === 'dependency'")
                      | dependency:&nbsp;
                      Job(v-if="cond.resolved_job_id" :job-id="cond.resolved_job_id")
                      span(v-else) {{ cond.job_id }}
                      span.dep-args(v-if="cond.resolved_args && Object.keys(cond.resolved_args).length > 0")
                        |  (
                        RunArgs(:args="cond.resolved_args")
                        | )
                      |  is {{ join(cond.states || ['success'], '|') }}
                    span(v-else) {{ cond.str }}
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
import { every, join, pickBy } from 'lodash'
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

    hasConditions() {
      if (!this.job) return false
      return this.job.condition.length > 0
    },

    hasParameterized() {
      return this.job && this.job.resolved_conditions && this.job.resolved_conditions.length > 0
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

.schedule-args {
  color: #888;
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
