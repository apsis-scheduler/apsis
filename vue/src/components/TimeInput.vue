<template lang="pug">
.time-input(ref="wrapper")
  .display(@click="toggleDropdown")
    span.value(v-if="displayValue") {{ displayValue }}
    span.placeholder(v-else) {{ nullable ? 'no limit' : 'now' }}
    TriangleIcon.caret(direction="down")

  .dropdown-anchor(v-if="showDropdown")
    .dropdown
      .dropdown-expr
        input.expr-field(
          ref="exprInput"
          v-model="expr"
          placeholder="e.g. now-30m, now, 2024-01-15 10:00"
          @keydown.enter.prevent="applyExpr"
          @keydown.escape="showDropdown = false"
        )
        button.expr-apply(@mousedown.prevent="applyExpr") Apply

      .dropdown-ranges(v-if="ranges && ranges.length")
        .range-item(
          v-for="range in ranges"
          :key="range.label"
          :class="{ active: isActiveRange(range) }"
          @mousedown.prevent="selectRange(range)"
        ) {{ range.label }}

      .dropdown-calendar
        .cal-header
          button.cal-nav(@mousedown.prevent="prevMonth") &#9664;
          span.cal-title {{ monthLabel }}
          button.cal-nav(@mousedown.prevent="nextMonth") &#9654;
        table.cal-grid
          thead
            tr
              th(v-for="d in ['Mo','Tu','We','Th','Fr','Sa','Su']") {{ d }}
          tbody
            tr(v-for="week in weeks")
              td(
                v-for="day in week"
                :class="dayClasses(day)"
                @mousedown.prevent="day.num && selectDay(day)"
              ) {{ day.num || '' }}

      .dropdown-clear(v-if="nullable")
        button.clear-btn(@mousedown.prevent="clearValue") Clear
</template>

<script>
import moment from 'moment-timezone'
import store from '@/store.js'
import { formatTimeExpr, isRelativeTimeExpr } from '../time'
import TriangleIcon from '@/components/icons/TriangleIcon'

export default {
  name: 'TimeInput',
  components: { TriangleIcon },

  props: {
    value: { default: null },
    nullable: { type: Boolean, default: false },
    // Quick range presets: [{ label, from, to }].
    // Selecting a range emits 'range' with { from, to }.
    ranges: { type: Array, default: null },
  },

  data() {
    const m = this.currentMoment()
    return {
      showDropdown: false,
      expr: this.valueToExpr(),
      viewYear: m.year(),
      viewMonth: m.month(),
      selectedDay: m.date(),
    }
  },

  computed: {
    timeZone() {
      return store.state.timeZone
    },

    displayValue() {
      return formatTimeExpr(this.value, this.timeZone)
    },

    monthLabel() {
      return moment({ year: this.viewYear, month: this.viewMonth }).format('MMMM YYYY')
    },

    weeks() {
      const first = moment.tz(
        { year: this.viewYear, month: this.viewMonth, day: 1 },
        this.timeZone
      )
      const startDay = first.isoWeekday()
      const daysInMonth = first.daysInMonth()
      const weeks = []
      let week = []

      for (let i = 1; i < startDay; i++)
        week.push({ num: 0 })

      for (let d = 1; d <= daysInMonth; d++) {
        week.push({ num: d })
        if (week.length === 7) {
          weeks.push(week)
          week = []
        }
      }

      if (week.length > 0) {
        while (week.length < 7)
          week.push({ num: 0 })
        weeks.push(week)
      }

      return weeks
    },
  },

  watch: {
    value() {
      this.expr = this.valueToExpr()
    },
  },

  methods: {
    currentMoment() {
      if (this.value && !isRelativeTimeExpr(this.value))
        return moment(this.value).tz(store.state.timeZone)
      return moment().tz(store.state.timeZone)
    },

    valueToExpr() {
      return formatTimeExpr(this.value, store.state.timeZone)
    },

    toggleDropdown() {
      this.showDropdown = !this.showDropdown
      if (this.showDropdown) {
        this.expr = this.valueToExpr()
        const m = this.currentMoment()
        this.viewYear = m.year()
        this.viewMonth = m.month()
        this.selectedDay = m.date()
        this.$nextTick(() => {
          if (this.$refs.exprInput)
            this.$refs.exprInput.focus()
        })
      }
    },

    applyExpr() {
      const input = this.expr.trim()
      if (!input) {
        if (this.nullable)
          this.emitValue(null)
        else
          this.emitValue('now')
        return
      }
      // Accept relative expressions as-is.
      if (input === 'now' || /^now[+-]\d+[smhd]$/.test(input)) {
        this.emitValue(input)
        return
      }
      // Try to parse as absolute time.
      const parsed = moment.tz(input, this.timeZone)
      if (parsed.isValid())
        this.emitValue(parsed.utc().format())
    },

    selectRange(range) {
      this.showDropdown = false
      this.$emit('range', { from: range.from, to: range.to })
    },

    isActiveRange(range) {
      return this.value === range.from || this.value === range.to
    },

    prevMonth() {
      if (this.viewMonth === 0) {
        this.viewMonth = 11
        this.viewYear--
      }
      else
        this.viewMonth--
    },

    nextMonth() {
      if (this.viewMonth === 11) {
        this.viewMonth = 0
        this.viewYear++
      }
      else
        this.viewMonth++
    },

    dayClasses(day) {
      if (!day.num) return ['empty']
      const classes = ['day']
      if (day.num === this.selectedDay
        && this.viewMonth === this.currentMoment().month()
        && this.viewYear === this.currentMoment().year())
        classes.push('selected')
      const today = moment().tz(this.timeZone)
      if (day.num === today.date()
        && this.viewMonth === today.month()
        && this.viewYear === today.year())
        classes.push('today')
      return classes
    },

    selectDay(day) {
      this.selectedDay = day.num
      const result = moment.tz({
        year: this.viewYear,
        month: this.viewMonth,
        day: this.selectedDay,
        hour: 0,
        minute: 0,
        second: 0,
      }, this.timeZone)
      if (result.isValid())
        this.emitValue(result.utc().format())
    },

    clearValue() {
      this.emitValue(null)
    },

    emitValue(val) {
      this.showDropdown = false
      if (val !== this.value) {
        this.$emit('input', val)
        this.$emit('change', val)
      }
    },

    onClickOutside(ev) {
      if (this.$refs.wrapper && !this.$refs.wrapper.contains(ev.target))
        this.showDropdown = false
    },
  },

  mounted() {
    document.addEventListener('mousedown', this.onClickOutside)
  },

  beforeDestroy() {
    document.removeEventListener('mousedown', this.onClickOutside)
  },
}
</script>

<style lang="scss" scoped>
@import '@/styles/vars.scss';

.time-input {
  position: relative;
  height: 100%;
}

.display {
  display: flex;
  align-items: center;
  height: 100%;
  border: 1px solid $global-frame-color;
  border-radius: 3px;
  padding: 0 8px;
  cursor: pointer;
  background: $global-background;
  font-size: 0.9em;
  gap: 4px;
  white-space: nowrap;

  &:hover {
    border-color: darken($global-frame-color, 15%);
  }
}

.value {
  font-family: 'Roboto Mono', monospace;
  font-size: 0.95em;
}

.placeholder {
  color: $global-light-color;
}

.caret {
  margin-left: auto;
  width: 1em;
}

.dropdown-anchor {
  position: absolute;
  top: 100%;
  left: 0;
  z-index: 20;
  margin-top: 2px;
}

.dropdown {
  background: white;
  border: 1px solid $global-frame-color;
  box-shadow: 4px 4px 8px rgba(0, 0, 0, 0.1);
  min-width: 280px;
}

.dropdown-expr {
  display: flex;
  padding: 8px;
  gap: 4px;
  border-bottom: 1px solid $global-frame-color;
}

.expr-field {
  flex: 1;
  font-size: 0.85em;
  padding: 4px 8px;
  border: 1px solid $global-frame-color;
  border-radius: 3px;
  font-family: 'Roboto Mono', monospace;

  &:focus {
    outline: none;
    border-color: $global-focus-color;
  }
}

.expr-apply {
  padding: 4px 10px;
  font-size: 0.8em;
  background: $global-select-background;
  border: 1px solid darken($global-frame-color, 5%);
  border-radius: 3px;
  cursor: pointer;
  white-space: nowrap;

  &:hover {
    background: darken($global-select-background, 5%);
  }
}

.dropdown-ranges {
  display: grid;
  grid-template-columns: 1fr 1fr;
  border-bottom: 1px solid $global-frame-color;
}

.range-item {
  padding: 5px 12px;
  font-size: 0.85em;
  cursor: pointer;
  white-space: nowrap;

  &:hover {
    background: $global-select-background;
  }

  &.active {
    color: $apsis-job-color;
    font-weight: 500;
  }
}

.dropdown-calendar {
  padding: 8px;
}

.cal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 4px;
}

.cal-nav {
  background: none;
  border: 1px solid $global-frame-color;
  border-radius: 3px;
  cursor: pointer;
  padding: 1px 8px;
  font-size: 0.8em;

  &:hover {
    background: $global-hover-background;
  }
}

.cal-title {
  font-weight: 500;
  font-size: 0.9em;
}

table.cal-grid {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 6px;

  th {
    font-size: 0.75em;
    color: $global-light-color;
    font-weight: normal;
    padding: 2px 0;
    text-align: center;
  }

  td {
    text-align: center;
    padding: 0;
    font-size: 0.85em;

    &.day {
      cursor: pointer;
      border-radius: 3px;
      padding: 3px 0;

      &:hover {
        background: $global-select-background;
      }
    }

    &.selected {
      background: $apsis-job-color;
      color: white;

      &:hover {
        background: darken($apsis-job-color, 10%);
      }
    }

    &.today:not(.selected) {
      font-weight: bold;
      border: 1px solid $apsis-job-color;
    }

    &.empty {
      cursor: default;
    }
  }
}

.dropdown-clear {
  padding: 6px 8px;
  border-top: 1px solid $global-frame-color;
}

.clear-btn {
  width: 100%;
  padding: 4px 8px;
  font-size: 0.85em;
  background: white;
  border: 1px solid $global-frame-color;
  border-radius: 3px;
  cursor: pointer;
  color: $global-light-color;

  &:hover {
    color: #c44;
    border-color: #c44;
  }
}
</style>
