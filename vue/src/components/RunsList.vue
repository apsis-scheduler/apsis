<template lang="pug">
div
  div.controls
    template(v-if="jobControls")
      div
        .label Job Path:
        PathNav(
          :path="path"
          :suggestions="allJobPaths"
          @path="path = $event"
        )

      div
        .label Keywords:
        WordsInput(
          v-model="keywords"
        )
        HelpButton
          p Syntax: <b>keyword keyword&hellip;</b>
          p Show only runs whose job ID contains each <b>keyword</b>.

      div.tags-filter-row
        .label Labels:
        TagsFilter(
          :value="labels"
          :suggestions="allLabels"
          label="label"
          @change="labels = $event"
        )
          HelpButton
            p Click <b>+ label</b> to add a label filter.
            p Show only runs with <b>all</b> selected labels.

    template(v-if="runControls")
      div.args-filter-row
        .label Run Args:
        ArgsFilter(
          :value="args"
          :params="effectiveParamNames"
          @change="args = $event"
        )
          HelpButton
            p Click <b>+ filter</b> to add a run args filter.
            p Multiple values for the same param: show runs matching <b>any</b> value.
            p Filters on different params: show runs matching <b>all</b> params.

      div
        .label States:
        StatesSelect(
          v-model="states"
        )

      div
        .label Repeated:
        div
          | Show
          Toggle.toggle(
            v-model="grouping"
          )
          | Hide &nbsp;
          HelpButton
            p How to present repeated runs, <i>i.e.</i> runs with the same job ID and run args.
            p <b>Show</b> each run individually.
            p <b>Hide</b> repeated runs, combined by run state:
              ul
                li Show the <i>latest completed</i> run; hide all previous completed runs.
                li Show the <i>earliest scheduled</i> run; hide all subsequent scheduled runs.
                li Show all runs in other states.
              | An additional column shows the number of hidden runs.

    template(v-if="timeControls")
      div.row-start
        .label From:
        div.time-field
          TimeInput(
            v-model="timeFrom"
            :nullable="true"
            :ranges="TIME_RANGES"
            @range="onTimeRange"
          )
          span.time-clear(v-if="timeFrom" @click="timeFrom = null") &times; clear

      div
        .label To:
        div.time-field
          TimeInput(
            v-model="timeTo"
            :nullable="true"
            :ranges="TIME_RANGES"
            @range="onTimeRange"
          )
          span.time-clear(v-if="timeTo" @click="timeTo = null") &times; clear

      div
        .label Order:
        div
          | Time &#8681;
          Toggle.toggle(
            v-model="asc"
          )
          | Time &#8679; &nbsp;
          HelpButton
            p Sort runs chronologically upward or downward.

      div
        .label Show:
        DropList.counts(
          :value="COUNTS.indexOf(show)"
          @input="show = COUNTS[$event]"
        )
          div(
            v-for="count in COUNTS"
          )
            div {{ count }} runs

      div
        .label Showing:
        div.showing-info
          span {{ groups.shownCount }} of {{ groups.totalCount }} runs
          span(v-if="groups.totalCount > groups.shownCount")  (limit: {{ show }})

      div &nbsp;


  div.runlist
    table.runlist
      colgroup
        col(v-if="showJob" style="min-width: 10rem")
        template(v-if="argColumnStyle === 'separate'")
          col(v-for="param in params")
        col(v-if="argColumnStyle === 'combined'" style="min-width: 10rem; max-width: 100%;")
        col(style="width: 4rem")
        col(style="width: 4rem")
        col(v-if="grouping" style="width: 5rem")
        col(style="width: 10rem")
        col(style="width: 10rem")
        col(style="width: 6rem")
        col(style="width: 10rem")
        col(style="width: 4rem")

      thead
        tr
          th.col-job(v-if="showJob") Job
          template(v-if="argColumnStyle === 'separate'")
            th.col-arg(v-for="param in params") {{ param }}
          th.col-args(v-if="argColumnStyle == 'combined'") Args
          th.col-run Run
          th.col-state State
          th.col-group(v-if="grouping") Hidden
          th.col-schedule-time Schedule
          th.col-start-time Start
          th.col-elapsed Elapsed
          th.col-stop-time Scheduled Stop
          th.col-operations Operations

      tbody
        tr(v-if="groups.groups.length == 0")
          td.note(colspan="9") No runs.
        tr(v-else)
          td.spacer(colspan="9")

        tr(v-if="(asc ? groups.earlierCount : groups.laterCount) > 0")
          td.note(colspan="9")
            | {{ asc ? groups.earlierCount : groups.laterCount }}
            | {{ asc ? 'earlier' : 'later' }} runs not shown
            button(@click="asc ? seeEarlier() : seeLater()") {{ asc ? 'Earlier' : 'Later' }}

        template(v-for="run, i in groups.groups")
          tr(v-if="nowIndicator && i === groups.nowIndex")
            td(colspan="9")
              .timeSeparator
                div.border
                div.now now
                div.border

          tr(:key="run.run_id")
            //- Show job name if enabled by 'showJob' and this is the group run.
            td.col-job(v-if="showJob")
              //- Is this the group run?
              Job(:job-id="run.job_id")
              JobLabel(
                v-for="label in run.labels || []"
                :label="label"
                :key="label"
              )

            template(v-if="argColumnStyle === 'separate'")
              td(v-for="param in params") {{ run.args[param] || '' }}
            //- Else all together.
            td.col-args(v-if="argColumnStyle === 'combined'")
              RunArgs(:args="run.args")

            td.col-run
              Run(:run-id="run.run_id")
            td.col-state
              State(:state="run.state")
            //- FIXME: Click to run with history expanded.
            td.col-group(v-if="grouping")
              router-link.hidden(
                :to="{ name: 'run', params: { run_id: run.run_id }, query: { runs: null } }"
              ) {{ historyCount(run, groups.counts[run.run_id]) }}
            td.col-schedule-time
              Timestamp(:time="run.times.schedule")
            td.col-start-time
              Timestamp(v-if="run.times.running" :time="run.times.running")
            td.col-elapsed
              RunElapsed(:run="run")
            td.col-stop-time
              Timestamp(v-if="run.times.stop" :time="run.times.stop" :class="{ overdue: isOverdue(run) }")
            td.col-operations
              HamburgerMenu(v-if="OPERATIONS[run.state].length > 0")
                OperationButton(
                  v-for="operation in OPERATIONS[run.state]"
                  :key="operation"
                  :run_id="run.run_id"
                  :operation="operation"
                  :button="true"
                )

        tr(v-if="(asc ? groups.laterCount : groups.earlierCount) > 0")
          td.note(colspan="9")
            | {{ asc ? groups.laterCount : groups.earlierCount }}
            | {{ asc ? 'later' : 'earlier' }} runs not shown
            button(@click="asc ? seeLater() : seeEarlier()") {{ asc ? 'Later' : 'Earlier' }}

</template>

<script>
import { entries, filter, flatten, groupBy, includes, isEqual, keys, map, sortBy, sortedIndexBy, uniq } from 'lodash'

import { matchKeywords, includesAll, OPERATIONS } from '@/runs'
import { formatDuration, formatElapsed, formatTime, resolveTimeExpr } from '@/time'
import ArgsFilter from '@/components/ArgsFilter'
import DropList from '@/components/DropList'
import TagsFilter from '@/components/TagsFilter'
import HamburgerMenu from '@/components/HamburgerMenu'
import HelpButton from '@/components/HelpButton'
import Job from '@/components/Job'
import JobLabel from '@/components/JobLabel'
import OperationButton from '@/components/OperationButton'
import PathNav from '@/components/PathNav'
import Run from '@/components/Run'
import RunArgs from '@/components/RunArgs'
import RunElapsed from '@/components/RunElapsed'
import State from '@/components/State'
import StatesSelect from '@/components/StatesSelect'
import store from '@/store.js'
import TimeInput from '@/components/TimeInput'
import Timestamp from '@/components/Timestamp'
import Toggle from '@/components/Toggle'
import WordsInput from '@/components/WordsInput'

const COUNTS = [20, 50, 100, 200, 500, 1000]

const TIME_RANGES = [
  { label: 'Last 5 minutes',  from: 'now-5m',  to: 'now' },
  { label: 'Last 15 minutes', from: 'now-15m', to: 'now' },
  { label: 'Last 30 minutes', from: 'now-30m', to: 'now' },
  { label: 'Last 1 hour',     from: 'now-1h',  to: 'now' },
  { label: 'Last 6 hours',    from: 'now-6h',  to: 'now' },
  { label: 'Last 24 hours',   from: 'now-24h', to: 'now' },
  { label: 'Last 7 days',     from: 'now-7d',  to: 'now' },
  { label: 'Last 30 days',    from: 'now-30d', to: 'now' },
  { label: 'Next 5 minutes',  from: 'now',     to: 'now+5m' },
  { label: 'Next 30 minutes', from: 'now',     to: 'now+30m' },
  { label: 'Next 1 hour',     from: 'now',     to: 'now+1h' },
  { label: 'Next 24 hours',   from: 'now',     to: 'now+24h' },
]

/**
 * Constructs a predicate fn for matching runs with `args`.
 *
 * `args` maps each param to an array of acceptable values.  A null value in
 * the array means any value is acceptable.  For each param, the run must match
 * at least one value (OR within a param).  All params must match (AND across
 * params).
 */
function getArgPredicate(args) {
  return run => {
    for (const param in args) {
      const value = run.args[param]
      if (value === undefined)
        return false
      if (!args[param].some(ref => ref === null || value === ref))
        return false
    }
    return true
  }
}

export default {
  name: 'RunsList',
  props: {
    query: {type: Object, default: null},

    // If true, show the job ID column.
    showJob: {type: Boolean, default: true},

    // If not null, highlight the run with this run ID.
    highlightRunId: {type: String, default: null},

    // How to indicate args:
    // - 'combined' for a single args column
    // - 'separate' for one column per param, suitable for runs of a single job
    // - 'none' for no args at all, suitable for runs of a single (job, args)
    argColumnStyle : {type: String, default: 'combined'},

    jobControls: {type: Boolean, default: true},
    runControls: {type: Boolean, default: true},
    timeControls: {type: Boolean, default: true},

    // Known parameter names for this job; enables the tag-based args filter.
    paramNames: {type: Array, default: null},

    // If true, show a row indicating the current time.
    nowIndicator: {type: Boolean, default: true},
  },

  components: {
    ArgsFilter,
    DropList,
    HamburgerMenu,
    HelpButton,
    Job,
    JobLabel,
    OperationButton,
    PathNav,
    Run,
    RunArgs,
    RunElapsed,
    State,
    StatesSelect,
    TimeInput,
    TagsFilter,
    Timestamp,
    Toggle,
    WordsInput,
  },

  data() {
    return {
      store,
      // If true, show profiling on console.log.
      profile: false,
      COUNTS,
      OPERATIONS,
      TIME_RANGES,

      args: null,        // no arg filters
      asc: false,         // show time descending
      grouping: true,    // hide repeated runs
      keywords: null,    // no keyword filters
      labels: null,      // no label filters
      path: null,        // job ID prefix
      job_id: null,      // exact job ID
      show: 50,
      states: null,      // all states
      timeFrom: null,    // start of time range, null = no lower bound
      timeTo: null,      // end of time range, null = no upper bound

      // Pagination offset into filtered results (ascending order).
      // null = auto (center around "now" when no time filter).
      windowStart: null,

      // Initialize with the query prop.
      ...this.query,
    }
  },

  computed: {
    /** Runs, after filtering.  */
    runs() {
      let runs = Array.from(this.store.state.runs.values())

      // Apply query filters in the order that seems most likely to put
      // the cheapest and most selective filters first.
      if (this.path) {
        const path = this.path
        const prefix = path + '/'
        runs = filter(runs, run => run.job_id === path || run.job_id.startsWith(prefix))
      }
      if (this.job_id)
        runs = filter(runs, run => run.job_id === this.job_id)
      if (this.states)
        runs = filter(runs, run => includes(this.states, run.state))
      if (this.labels)
        runs = filter(runs, run => run.labels && includesAll(this.labels, run.labels))
      if (this.args)
        runs = filter(runs, getArgPredicate(this.args))
      if (this.keywords) {
        const keywords = this.keywords.map(s => s.toLowerCase())
        runs = filter(runs, run => matchKeywords(keywords, run.job_id.toLowerCase()))
      }

      return sortBy(runs, r => r.time_key)
    },
  
    params() {
      return uniq(flatten(map(this.runs, run => keys(run.args))))
    },

    allParamNames() {
      const names = new Set()
      for (const run of this.store.state.runs.values())
        for (const k of keys(run.args))
          names.add(k)
      return Array.from(names).sort()
    },

    effectiveParamNames() {
      return this.paramNames || this.allParamNames
    },

    allLabels() {
      const labels = new Set()
      for (const run of this.store.state.runs.values())
        if (run.labels)
          for (const l of run.labels)
            labels.add(l)
      return Array.from(labels).sort()
    },

    allJobPaths() {
      const paths = new Set()
      for (const run of this.store.state.runs.values()) {
        const parts = run.job_id.split('/')
        // Add directory prefixes only, not the full job ID.
        for (let i = 1; i < parts.length; i++)
          paths.add(parts.slice(0, i).join('/'))
      }
      return Array.from(paths).sort()
    },

    // Array of rerun groups, each an array of runs that are reruns of the
    // same run.  Groups are filtered by current filters, and sorted.
    allGroups() {
      let t0 = new Date()
      let t1

      let groups
      let counts = {}
      if (this.grouping) {
        const runs = this.runs
        if (this.profile) {
          t1 = new Date()
          console.log('groups-runs', (t1 - t0) * 0.001)
          t0 = t1
        }

        // For each group, select the principal run for the group to show.
        groups = map(entries(groupBy(runs, r => r.group_key)), ([key, runs]) => {
          // Select the principal run for this group.
          // - new/scheduled: the earliest run
          // - blocked, running: not grouped
          // - completed: the latest run
          const sgrp = key[0]
          const run = runs[sgrp === 'C' ? runs.length - 1 : 0]

          counts[run.run_id] = runs.length
          return run
        })

        if (this.profile) {
          t1 = new Date()
          console.log('groups-groupBy', (t1 - t0) * 0.001)
          t0 = t1
        }
      }

      else {
        groups = this.runs
        groups.forEach(r => { counts[r.run_id] = 1 })
      }

      // Sort groups by time.
      groups = sortBy(groups, r => r.time_key)

      return {
        groups,
        counts,
      }
    },

    groups() {
      const groups = this.allGroups
      let runs = groups.groups

      // Touch store.state.time so this recomputes every second for relative exprs.
      void this.store.state.time
      const now = (new Date()).toISOString()
      const from = resolveTimeExpr(this.timeFrom)
      const to = resolveTimeExpr(this.timeTo)

      // Filter runs to the [from, to] time range.
      if (from)
        runs = runs.filter(r => r.time_key >= from)
      if (to)
        runs = runs.filter(r => r.time_key <= to)

      const totalCount = runs.length
      let earlierCount = 0
      let laterCount = 0

      // Apply the show limit using windowStart for pagination.
      if (runs.length > this.show) {
        let start
        if (this.windowStart !== null)
          // Use the explicit pagination offset.
          start = Math.max(0, Math.min(this.windowStart, runs.length - this.show))
        else {
          // Auto: center around "now" to show both recent and upcoming runs.
          const center = sortedIndexBy(runs, { time_key: now }, r => r.time_key)
          const half = Math.floor(this.show / 2)
          start = Math.max(0, center - half)
          if (start + this.show > runs.length)
            start = Math.max(0, runs.length - this.show)
        }

        const end = Math.min(start + this.show, runs.length)
        earlierCount = start
        laterCount = runs.length - end
        runs = runs.slice(start, end)
      }

      const shownCount = runs.length

      // Find where "now" falls in the visible runs for the now indicator.
      let nowIndex
      if (runs.length > 0 && runs[0].time_key < now && now < runs[runs.length - 1].time_key)
        nowIndex = sortedIndexBy(runs, { time_key: now }, r => r.time_key)

      if (!this.asc) {
        runs.reverse()
        if (nowIndex)
          nowIndex = runs.length - nowIndex
      }

      return {
        groups: runs,
        counts: groups.counts,
        nowIndex,
        totalCount,
        shownCount,
        earlierCount,
        laterCount,
      }
    },
  },

  watch: {
    // If parent updates the query prop, update our state correspondingly.
    query: {
      deep: true,
      handler(query) {
        for (const key of Object.keys(query)) {
          const n = query[key]
          if (!isEqual(n, this[key]))
            this.$set(this, key, n)
        }
      },
    },

    // Whenever our query state changes, inform the parent and reset pagination.
    args() { this.windowStart = null; this.emitQuery() },
    asc() { this.windowStart = null; this.emitQuery() },
    grouping() { this.windowStart = null; this.emitQuery() },
    keywords() { this.windowStart = null; this.emitQuery() },
    labels() { this.windowStart = null; this.emitQuery() },
    path() { this.windowStart = null; this.emitQuery() },
    show() { this.windowStart = null; this.emitQuery() },
    states() { this.windowStart = null; this.emitQuery() },
    timeFrom() { this.windowStart = null; this.emitQuery() },
    timeTo() { this.windowStart = null; this.emitQuery() },

  },

  methods: {
    formatElapsed,

    onTimeRange({ from, to }) {
      this.timeFrom = from
      this.timeTo = to
    },

    seeEarlier() {
      const current = this.windowStart !== null ? this.windowStart : this.groups.earlierCount
      this.windowStart = Math.max(0, current - this.show)
    },

    seeLater() {
      const current = this.windowStart !== null ? this.windowStart : this.groups.earlierCount
      this.windowStart = current + this.show
    },

    // Sends our current state to the parent as a query object.
    emitQuery() {
      this.$emit('query', {
        path: this.path,
        states: this.states,
        labels: this.labels,
        args: this.args,
        keywords: this.keywords,
        show: this.show,
        timeFrom: this.timeFrom,
        timeTo: this.timeTo,
        grouping: this.grouping,
        asc: this.asc,
      })
    },

    historyCount(run, count) {
      return (
        count === 1 ? '' 
        : '+' + (count - 1) + ' ' + (
          run.group_key.startsWith('S') ? 'scheduled'
          : 'completed'
        )
      )
    },

    formatTime(time) {
      return time ? formatTime(time, this.store.state.timeZone) : '\u00a0'
    },

    startTime(run) {
      if (run.times.schedule) {
        const now = this.store.state.time
        const schedule = new Date(run.times.schedule)
        return formatDuration(Math.round((schedule - now) * 1e-3))
      }
      else
        return ''
    },

    isOverdue(run) {
      return run.times.stop
        && (   run.state === 'starting'
            || run.state === 'running'
            || run.state === 'stopping')
        && new Date(run.times.stop) < new Date()
    },
  },

}
</script>

<style lang="scss" scoped>
@import '@/styles/index.scss';

.runs-list {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
}

.controls {
  width: 80em;
  margin-top: 1em;
  margin-bottom: 2em;
  background: $global-control-background;
  padding: 12px 18px;

  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px 2em;
  justify-items: left;
  align-items: baseline;

  white-space: nowrap;
  line-height: 28px;

  > div {
    display: grid;
    height: 30px;
    width: 100%;
    grid-template-columns: 5em 1fr 1em;
    justify-items: left;
    justify-content: flex-start;
    align-items: center;
    gap: 4px;

    > div:not(.label) {
      height: 100%;
    }
  }

  > .args-filter-row, > .tags-filter-row {
    grid-column: 1 / -1;
    height: auto;
    min-height: 30px;
    grid-template-columns: 5em 1fr;
    align-items: start;
    padding-top: 2px;
  }

  .label {
    text-align: right;
    white-space: nowrap;
  }

  .field {
    display: inline-block;
    border: 1px solid $apsis-frame-color;
    text-align: center;
    padding: 0 12px;
    &.disabled {
      border: none;
    }
  }

  .toggle {
    margin: 0 1ch;
  }

  button {
    text-align: center;
    height: 100%;

    svg {
      height: 100%;
      vertical-align: center;
    }
  }

  input {
    background-color: $global-background;
  }

  input[type="checkbox"] {
    width: 16px;
    height: 16px;
  }

  .counts {
    width: 6em;
    height: 32px;
    text-align: right;
  }

  .time-field {
    display: flex;
    align-items: center;
    gap: 4px;
    height: 100%;
  }

  .time-clear {
    cursor: pointer;
    color: $global-light-color;
    font-size: 0.85em;
    border: 1px dashed $global-frame-color;
    border-radius: 3px;
    padding: 1px 8px;
    line-height: 22px;
    white-space: nowrap;

    &:hover {
      color: #c44;
      border-color: darken($global-frame-color, 10%);
      background: $global-hover-background;
    }
  }

  .showing-info {
    font-size: 0.9em;
    color: $global-light-color;
  }

  .row-start {
    grid-column-start: 1;
  }
}

table.runlist {
  @extend .widetable;

  thead {
    position: sticky;
  }

  .spacer {
    height: 0;
  }

  .note {
    height: 40px;

    button {
      margin-left: 8px;
      padding: 0 12px;
      line-height: 20px;
      height: 24px;
    }
  }

  .col-job, .col-args, .col-arg {
    text-align: left;
  }

  .col-run, .col-state, .col-operations {
    text-align: center;
  }

  .col-state {
    vertical-align: bottom;
  }

  .col-schedule-time, .col-start-time, .col-stop-time {
    font-size: 90%;
    color: $global-light-color;
    text-align: right;
  }

  .col-stop-time .overdue {
    color: #b07040;
  }

  .col-group {
    text-align: right;
    white-space: nowrap;
    color: $global-light-color;
    font-size: 90%;

    & > span {
      // Absolutely no idea why this is necessary.
      position: relative;
      top: -4px;
    }
  }

  .col-elapsed {
    padding-right: 1em;
    text-align: right;
    white-space: nowrap;
  }

  .highlight-run {
    background: #f6faf8;
  }

  .hidden {
    color: $global-light-color;
    cursor: default;
    &:hover {
      text-decoration: underline;
    }
  }

  .timeSeparator {
    width: 100%;
    display: flex;

    .border {
      flex-basis: 50%;
      border-bottom: 2px dotted $global-frame-color;
      margin-bottom: 0.5em;
      line-height: 50%;
    }

    .now {
      margin: 0 8px;
      color: $global-frame-color;
    }
  }
}
</style>
