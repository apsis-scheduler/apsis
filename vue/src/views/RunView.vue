<template lang="pug">
div
  div.runs(v-if="run")
    div.title
      | Run {{ run_id }}
    div.subhead
      JobWithArgs(:job-id="run.job_id" :args="run.args")
      span(v-if="run && run.meta && run.meta.job && run.meta.job.labels")
        JobLabel.label(v-for="label in run.meta.job.labels" :key="label" :label="label")

    div.buttons.row-centered
      State.state(:state="run.state" name)
      OperationButton(
        v-for="operation in OPERATIONS[run.state]"
        v-if="operation !== 'stop' || (run.times && run.times.stop !== undefined && run.times.stop !== null)"
        :key="operation"
        :run_id="run_id"
        :operation="operation"
        :button="true"
      )

    Frame(
      title="Run History"
      :closed="isCollapsed.runs"
    )
      RunsList(
        :show-job="false"
        :max-completed-runs="12"
        :max-scheduled-runs="12"
        arg-column-style="separate"
        :highlight-run-id="run.run_id"
        :job-controls="false"
        :run-controls="false"
        :time-controls="true"
        :query="{show: 20, grouping: false, job_id: run.job_id, args: run.args || {}}"
        style="max-height: 60rem; overflow-y: auto;"
      )

    Frame(title="Details")
      div
        table.fields
          tbody
            tr(v-if="run.message")
              th message
              td {{ run.message }}

            tr(v-if="run.program")
              th program
              td.no-padding: Program(:program="run.program")

            tr
              th conditions
              td.no-padding: table.fields: tbody
                tr(v-for="cond in run.conds" :key="cond.str")
                  td(style="padding-left: 0") {{ cond.str }}

            tr
              th elapsed
              td
                RunElapsed(:run="run")

            tr(v-if="run.times.stop")
              th scheduled stop
              td: Timestamp(:time="run.times.stop")

    Frame(title="Run Log")
      div
        table.fields.run-log
          tbody
            tr(v-for="rec of runLog" :key="rec.timestamp + rec.message")
              th: Timestamp(:time="rec.timestamp")
              td {{ rec.message }}

    Frame(title="Dependencies")
      RunDependencies(:run="run")

    Frame(title="Metadata" closed)
      div.objects
        template(v-for="(value, key) in meta && meta.program || {}")
          div.key {{ key }}
          div.objects(v-if="typeof value === 'object' && !Array.isArray(value)")
            template(v-for="(v, k) in value")
              div.key {{ k }}
              div.value {{ v === null ? 'null' : v }}
          div.value(v-else) {{ value === null ? 'null' : value }}

    Frame(title="Output" v-if="hasOutput")
      div.output(v-if="outputMetadata")
        div.head
          span.name {{ outputMetadata.name }}
          span {{ outputMetadata.length }} bytes
          button.scroll(v-on:click="scrollTo('top')" :disabled="outputData === null")
            DoubleChevronIcon(:direction="'up'")
          button.scroll(v-on:click="scrollTo('end')" :disabled="outputData === null")
            DoubleChevronIcon(:direction="'down'")
        div.data(v-if="outputData !== null" ref="outputData")
          pre.pad {{ outputData }}
          div#anchor  <!-- to pin bottom scrolling -->

  div.error-message(v-else-if="run === undefined")
    div.title
      | {{ run_id }}

    | This run does not currently exist.
    | This may be a run that was previously scheduled but never run.

</template>

<script>
import { join } from 'lodash'

import * as api from '@/api'
import Frame from '@/components/Frame'
import DoubleChevronIcon from '@/components/icons/DoubleChevronIcon'
import Job from '@/components/Job'
import JobLabel from '@/components/JobLabel'
import JobWithArgs from '@/components/JobWithArgs'
import OperationButton from '@/components/OperationButton'
import Program from '@/components/Program'
import Run from '@/components/Run'
import { isComplete, OPERATIONS } from '@/runs'
import RunArgs from '@/components/RunArgs'
import RunDependencies from '@/components/RunDependencies'
import RunElapsed from '@/components/RunElapsed'
import RunsList from '@/components/RunsList'
import State from '@/components/State'
import Timestamp from '@/components/Timestamp'
import store from '@/store'
import { Socket, parseHttp } from '@/websocket.js'

export default {
  props: ['run_id'],
  components: {
    OperationButton,
    Frame,
    Job,
    JobLabel,
    JobWithArgs,
    Program,
    Run,
    RunArgs,
    RunDependencies,
    RunElapsed,
    RunsList,
    State,
    Timestamp,
    DoubleChevronIcon,
  },

  data() {
    return {
      isCollapsed: {
        runs: this.$route.query.runs !== null,
      },
      OPERATIONS,
      run: null,
      runLog: null,
      outputMetadata: null,
      outputData: null,
      outputDataRequested: false,
      // Run metadata.
      meta: null,
      store,
      runUpdateSocket: null,
      outputDataUpdateSocket: null,
    }
  },

  computed: {
    /**
     * The update sequence of the run summary in the store, for detecting updates.
     */
    updateSeq() {
      const run = store.state.runs.get(this.run_id)
      return run ? run.seq : undefined
    },

    runState() {
      return this.run ? this.run.state : null
    },

    hasOutput() {
      const state = this.runState
      return state === 'running' || state === 'stopping' || isComplete(state)
    },

    metadata() {
      return this.metadata ? this.metadata.program || {} : {}
    },
  },

  methods: {
    join,

    fetchRun() {
      const url = api.getRunUrl(this.run_id)
      fetch(url)
        .then(async (response) => {
          if (response.ok) {
            const run = (await response.json()).runs[this.run_id]
            this.run = Object.freeze(run)
          }
          else if (response.status === 404)
            this.run = undefined
          else
            store.state.errors.add('fetch ' + url + ' ' + response.status + ' ' + await response.text())
        })
    },

    getRunUpdates() {
      if (this.runUpdateSocket) {
        this.runUpdateSocket.close()
        this.runUpdateSocket = null
      }

      // Connect to live run updates.
      const url = api.getRunUpdatesUrl(this.run_id, true)
      this.runUpdateSocket = new Socket(
        url,
        msg => {
          msg = JSON.parse(msg.data)
          console.log('run msg:', Object.keys(msg).join(' '))
          if (msg.meta)
            this.meta = msg.meta
          if (msg.run_log)
            this.runLog = msg.run_log
          if (msg.run_log_append)
            this.runLog.push(msg.run_log_append)
          if (msg.outputs && msg.outputs['output'])
            this.outputMetadata = msg.outputs['output']
        },
        () => {},
        err => { console.log(err) },
        () => {},
        { },
      )
      this.runUpdateSocket.open()
    },

    /**
     * Fetches output data from the HTTP endpoint.
     */
    getOutputData() {
      const url = api.getOutputDataUrl(this.run.run_id, 'output')

      // Don't request output more than once.
      if (this.outputDataRequested)
        return
      this.outputDataRequested = true

      fetch(url)
        // FIXME: Might not be text!
        // FIXME: Don't murder the browser with huge output or long lines.
        .then(async rsp => {
          this.outputData = await rsp.text()
        })
        // FIXME: Handle error.
        .catch(err => { console.log(err) })
    },

    stopOutputDataUpdates() {
      if (this.outputDataUpdateSocket) {
        this.outputDataUpdateSocket.close()
        this.outputDataUpdateSocket = null
      }
    },

    startOutputDataUpdates() {
      this.stopOutputDataUpdates()

      const url = api.getOutputDataUpdatesUrl(this.run_id, 'output', 0)
      this.outputDataUpdateSocket = new Socket(
        url,
        msg => {
          const decoder = new TextDecoder()
          msg.data.arrayBuffer().then(b => {
            let [headers, body] = parseHttp(b)
            let [ , start, , ] = headers['content-range'].match(/bytes=(\d+)-(\d+)\/(\d+)/)
            start = parseInt(start)
            body = decoder.decode(body)
            if (this.outputData === null)
              this.outputData = body
            else {
              if (start !== this.outputData.length)
                console.error('output data fragmentation:', this.outputData.length, start)
              this.outputData += body
            }
          })
        },
        () => {},
        err => { console.log(err) },
        () => {},
        { },
      )
      this.outputDataUpdateSocket.open()
    },

    initRun() {
      // The run may not yet be in the store, if the site was just loaded.
      // Don't treat this as a missing run (this.run === undefined) until the
      // single run API can't find it.
      this.run = store.state.runs.get(this.run_id) || null

      this.outputMetadata = null
      this.outputData = null
      this.outputDataRequested = false

      this.fetchRun()
      this.getRunUpdates()
    },

    scrollTo(where) {
      const el = this.$refs.outputData
      el.scrollTo(0, where === 'top' ? 0 : el.scrollHeight)
    },
  },

  beforeMount() {
    this.initRun()
  },

  beforeDestroy() {
    // Disconnect live run updates.
    this.runUpdateSocket.close()
    this.runUpdateSocket = null

    this.stopOutputDataUpdates()
  },

  watch: {
    // Reset state on nav from one run to another.
    '$route'(to, from) {
      this.initRun()
    },

    // When the update sequence number changes, reload the whole run.
    updateSeq(seq, previous) {
      if (!this.run || this.run.seq !== seq)
        this.fetchRun()
    },

    runState(to, from) {
      if (to === 'running')
        // Connect for live output data.
        this.startOutputDataUpdates()
      else
        this.stopOutputDataUpdates()

      if (isComplete(to) && (this.outputData === null || this.outputData.length < this.outputMetadata.length))
        // Fetch output data.
        this.getOutputData('output')
    },
  },

}
</script>

<style lang="scss" scoped>
@import '@/styles/index.scss';

.title {
  margin-bottom: 0.5rem;
}

.subhead {
  margin-bottom: 1rem;
  font-size: 130%;

  .label {
    font-size: 80%;
  }
}

.buttons {
  margin-bottom: 1.5rem;
}

.output {
  .head {
    padding-top: 0.5em;
    padding-bottom: 1em;

    span {
      display: inline-block;
      padding-right: 0.5em;
      &.name {
        font-weight: bold;
      }
    }

    button {
      margin: 0 0.2em;
      padding: 0 0.2em;
      background: none;
      border: 1px solid transparent;

      &:hover {
        border: 1px solid $global-button-color;
      }
    }
  }

  .data {
    max-height: 50vh;
    margin-bottom: 0.5em;

    overflow-y: scroll;
    * {
      overflow-anchor: none;
    }

    pre {
      padding: 0;
      font-family: "Roboto mono", monospaced;
    }

    #anchor {
      overflow-anchor: auto;
      height: 1px;
    }
  }
}

.state {
  font-weight: bold;
  margin-right: 2ex;
}

.subfields {
  font-size: 85%;

  th {
    font-size: 85%;
    min-width: 24ex;
  }

  th, td {
    line-height: 1.1rem;
    padding-top: 0;
    padding-bottom: 0;
  }
}

.run-log {
  margin-top: 0.75rem;
  margin-bottom: 0.75rem;
  th, td {
    line-height: 1.5rem;
    padding-top: 0 !important;
    padding-bottom: 0 !important;
  }
}

.objects {
  display: grid;
  grid-template-columns: minmax(12em, max-content) max-content;

  .key, .value {
    padding: 2px 0;
  }

  .key {
    font-size: 0.875rem;
    font-weight: normal;
    text-transform: uppercase;
    color: #999;
  }

  .value {
    font-family: $base-code-font-family;
    font-size: 90%;
  }
}
</style>

