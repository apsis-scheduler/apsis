import { filter, isEqual, some } from 'lodash'
import { formatTime } from '@/time'

class Errors {
  // FIXME: I'm using  a list here because I can't figure out how to get
  // watch behavior working with an object keyed by errorId.
  errors = []
  nextErrorId = 0

  push(message) {
    // Suppress duplicate messages.
    if (some(this.errors, e => e.message === message))
      return

    const errorId = this.nextErrorId++
    this.errors.push({errorId, message, time: new Date()})
    return errorId
  }

  pop(message) {
    this.errors = filter(this.errors, e => e.message !== message)
  }

  clear(errorId) {
    for (let i = 0; i < this.errors.length; i++)
      if (this.errors[i].errorId === errorId) {
        this.errors.splice(i, 1)
        break
      }
  }
}

class Store {
  state = {
    errors: new Errors(),

    // Map from run_id to (summary) runs.  Updated live.
    runs: new Map(),

    // Current time, updated secondly, and its string representation.
    time: new Date(),
    timeStr: '',

    // User-selected time zone.
    timeZone: 'UTC',

    // Array of jobs.  Update live.
    jobs: new Map(),
    // Expand state in the job tree.
    jobsExpand: {},

    // Array of Procstar agent connections.
    agentConns: new Map(),
  }

  setTime(time) {
    this.state.time = time || new Date()
    this.state.timeStr = formatTime(this.state.time, this.state.timeZone)
  }

  setTimeZone(timeZone) {
    this.state.timeZone = timeZone
    this.state.timeStr = formatTime(this.state.time, this.state.timeZone)
  }

  getRunsForInstance(job_id, args) {
    // FIXME: Optimize.
    return Array.from(
      this.state.runs.values()
        .filter(r => r.job_id === job_id && isEqual(r.args, args))
    )
  }

  _tick() {
    this.setTime()
    // Update every second, on the second.
    window.setTimeout(() => this._tick(), 1000 - this.state.time % 1000) 
  }

  constructor() {
    this._tick()
  }
}

const store = new Store()
export default store
