<template lang="pug">
div
  div.controls
    template(v-if="jobControls")
      div
        .label Job Path:
        PathNav(
          :path="path"
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

      div
        .label Labels:
        WordsInput(
          v-model="labels"
        )
        HelpButton
          p Syntax: <b>label label&hellip;</b>
          p Show only runs with each <b>label</b>.

    template(v-if="runControls")
      div
        .label Run Args:
        WordsInput(
          :value="args !== null ? argsToArray(args) : null"
          @change="args = $event ? arrayToArgs($event) : null"
        )
        HelpButton
          p Syntax: <b>arg=value arg=value&hellip;</b>
          p Show only runs which have all run args <b>arg</b> set to corresponding <b>value</b>.

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
      //- Number of runs to show.
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

      //- Show Time and earier / now / later buttons.
      div
        .label Time:
        div(style="display: flex; height: 100%;")
          button(
            style="padding: 0 4px; border-top-right-radius: 0; border-bottom-right-radius: 0;"
            v-on:click="time = groups.earlierTime"
            :disabled="groups.earlierCount == 0"
          )
            TriangleIcon(
              direction="left"
            )
          TimeInput(
            v-model="time"
          )
          button(
            style="padding: 0 4px; border-top-left-radius: 0; border-bottom-left-radius: 0;"
            v-on:click="time = groups.laterTime"
            :disabled="groups.laterCount == 0"
          )
            TriangleIcon(
              direction="right"
            )
        HelpButton
          p Show the runs nearest this time, immediately before and after.
          p <b>Now</b> tracks the current time.
          p Specify another date and/or time. The arrows move backward or forward in time.

      div &nbsp;

      div
        .label Order:
        div
          | Time &#8681;
          Toggle.toggle(
            v-model="asc"
          )
          | Time &#8679; &nbsp;
          HelpButton
            p Time runs chronologically upward or downward.
        div &nbsp;

      div
        .label Showing:
        div(style="display: grid; width: 100%; grid-template-columns: 1fr 1em 1fr;")
          span(style="justify-self: start") {{ formatTime(groups.earlierTime) }}
          span &mdash;
          span(style="justify-self: end") {{ formatTime(groups.laterTime) }}
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
            | {{ asc ? 'earlier' : 'later' }} rows not shown
            button(
              v-on:click="time = asc ? groups.earlierTime : groups.laterTime"
            ) {{ asc ? 'Earlier' : 'Later' }}

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
            | {{ asc ? 'later' : 'earlier' }} rows not shown
            button(
              v-on:click="time = asc ? groups.laterTime : groups.earlierTime"
            ) {{ asc ? 'Later' : 'Earler' }}

</template>

<script>
import { entries, filter, flatten, groupBy, includes, isEqual, keys, map, sortBy, sortedIndexBy, uniq } from 'lodash'

import { argsToArray, arrayToArgs, matchKeywords, includesAll, OPERATIONS } from '@/runs'
import { formatDuration, formatElapsed, formatTime } from '@/time'
import DropList from '@/components/DropList'
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
import TriangleIcon from '@/components/icons/TriangleIcon'
import WordsInput from '@/components/WordsInput'

const COUNTS = [20, 50, 100, 200, 500, 1000]

/**
 * Constructs a predicate fn for matching runs with `args`.
 *
 * `args` is an array of "param=value" or "param" strings.  The former requires
 * the given param have the cooresponding arg value.  The latter requires that
 * it have any arg value at all.  The result is the conjunction of these.
 *
 * For example, the `args` array ["fruit=mango", "color"] produced a predicate
 * that matches all runs that both have a "fruit" param with arg value "mango",
 * and also have any value at all ofr the "color" param.
 */
function getArgPredicate(args) {
  return run => {
    for (const param in args) {
      const value = run.args[param]
      if (value === undefined)
        // Not present.
        return false
      const ref = args[param]
      if (ref !== null && value !== ref)
        // Wrong arg value.
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

    // If true, show a row indicating the current time.
    nowIndicator: {type: Boolean, default: true},
  },

  components: {
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
    Timestamp,
    Toggle,
    TriangleIcon,
    WordsInput,
  },

  data() {
    return {
      store,
      // If true, show profiling on console.log.
      profile: false,
      COUNTS,
      OPERATIONS,

      args: null,        // no arg filters
      asc: false,         // show time descending
      grouping: true,    // hide repeated runs
      keywords: null,    // no keyword filters
      labels: null,      // no label filters
      path: null,        // job ID prefix
      job_id: null,      // exact job ID
      show: 50,
      states: null,      // all states
      time: 'now',

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
        runs = filter(runs, run => includesAll(this.labels, run.labels))
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
      let start = new Date()
      const groups = this.allGroups
      let runs = groups.groups   // FIXME

      let now = (new Date()).toISOString()

      // Determine the time to center around.
      const time = this.time === 'now' ? now : this.time
      // Find the index corresponding to the center time.
      let timeIndex = sortedIndexBy(runs, { time_key: time }, r => r.time_key)

      // Time cutoff and count of earlier runs not shown.
      let earlierTime = null
      let earlierCount = 0
      // Time cutoff and count of later runs not shown.
      let laterTime = null
      let laterCount = 0
      const show = this.show
      if (show < runs.length) {
        // There are more runs than fit in the view.  Decide how many runs to
        // show before and after the center time.
        var r0 = timeIndex
        var r1 = runs.length - timeIndex
        if (r0 < show / 2)
          r1 = show - r0
        else if (r1 < show / 2)
          r0 = show - r1
        else
          r0 = r1 = show / 2

        if (r0 < timeIndex) {
          // Don't show some runs and omit others with identical timestamp.
          while (
            0 < timeIndex && timeIndex < runs.length
            && runs[timeIndex - 1].time_key === runs[timeIndex].time_key
          )
            timeIndex--
          // Number of earlier runs omitted.
          earlierCount = timeIndex - r0
          // Time cutoff of earlier runs omitted.
          earlierTime = runs[earlierCount].time_key
          // Omit the runs.
          runs = runs.slice(earlierCount)
          // Adjust the center time index accordingly.
          timeIndex -= earlierCount
        }

        if (r1 < runs.length - timeIndex) {
          // Don't show some runs and omit others with identical timestamp.
          while (
            0 < timeIndex && timeIndex < runs.length - 1
            && runs[timeIndex - 1].time_key === runs[timeIndex].time_key
          )
            timeIndex++
          // Number of later runs omitted.
          laterCount = runs.length - (timeIndex + r1)
          // Time cutoff of later runs omitted.
          laterTime = runs[runs.length - laterCount].time_key
          // Omit the runs.
          runs = runs.slice(0, runs.length - laterCount)
        }
      }

      var nowIndex
      if (runs.length > 0 && runs[0].time_key < now && now < runs[runs.length - 1].time_key)
        nowIndex = sortedIndexBy(runs, { time_key: now }, r => r.time_key)

      if (!this.asc) {
        runs.reverse()
        if (nowIndex)
          nowIndex = runs.length - nowIndex
      }

      if (this.profile)
        console.log(
          'runs:', this.store.state.runs.size, 'filtered:', this.runs.length, 'groups:', runs.length, 
          'earlier:', earlierCount, earlierTime,
          'later:', laterCount, laterTime,
          'in:', (new Date() - start) * 0.001
        )

      return {
        groups: runs,
        counts: groups.counts,
        nowIndex,
        time,
        earlierTime,
        earlierCount,
        laterTime,
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

    // Whenever our query state changes, inform the parent.
    args() { this.emitQuery() },
    asc() { this.emitQuery() },
    grouping() { this.emitQuery() },
    keywords() { this.emitQuery() },
    labels() { this.emitQuery() },
    path() { this.emitQuery() },
    show() { this.emitQuery() },
    states() { this.emitQuery() },
    time() { this.emitQuery() },

  },

  methods: {
    formatElapsed,
    arrayToArgs,
    argsToArray,

    // Sends our current state to the parent as a query object.
    emitQuery() {
      this.$emit('query', {
        path: this.path,
        states: this.states,
        labels: this.labels,
        args: this.args,
        keywords: this.keywords,
        show: this.show,
        time: this.time,
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
