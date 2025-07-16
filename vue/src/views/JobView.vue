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
        td(v-if="job.condition.length > 0")
          .condition(v-for="cond in job.condition" :key="cond.str")
            span(v-if="cond.type === 'dependency'")
              span dependency: 
              Job(:job-id="cond.job_id")
              span  is {{ join(cond.states, '|') }}
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
