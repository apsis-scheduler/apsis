<template lang="pug">
.args-filter
  .filter-tag(
    v-for="(tag, i) in tags"
    :key="tag.param + '-' + i"
  )
    span.tag-param {{ tag.param }}
    span.tag-eq =
    span.tag-value {{ tag.value === null ? '*' : tag.value }}
    span.tag-remove(@click="removeFilter(tag.param, tag.value)") &times;

  .value-input(v-if="pendingParam")
    span.pending-param {{ pendingParam }} =
    input.pending-value(
      ref="valueInput"
      v-model="pendingValue"
      placeholder="value"
      maxlength="255"
      @keydown.enter="commitFilter"
      @keydown.escape="cancelPending"
    )
    button.pending-ok(@click="commitFilter") OK
    button.pending-cancel(@click="cancelPending") Cancel

  .add-filter(ref="addFilter")
    span.action-button(@click="toggleDropdown")
      | + filter
    .dropdown(v-if="showDropdown")
      input.dropdown-search(
        ref="searchInput"
        v-model="searchText"
        placeholder="type to search..."
        @keyup.escape="showDropdown = false"
        @keydown.enter.prevent="selectHighlighted"
        @keydown.down.prevent="moveHighlight(1)"
        @keydown.up.prevent="moveHighlight(-1)"
      )
      .dropdown-item(
        v-for="(param, i) in filteredParams"
        :key="param"
        :class="{ highlighted: i === highlightIndex }"
        @mousedown.prevent="selectParam(param)"
        @mouseenter="highlightIndex = i"
      ) {{ param }}
      .dropdown-empty(v-if="filteredParams.length === 0") No matching params
  span.action-button.action-clear(v-if="value" @click="clearAll") &times; clear
  slot
</template>

<script>
export default {
  name: 'ArgsFilter',

  props: {
    // The current filter: {param: [value, ...], ...} or null.
    value: { type: Object, default: null },
    // Available parameter names.
    params: { type: Array, default: () => [] },
  },

  data() {
    return {
      showDropdown: false,
      searchText: '',
      highlightIndex: 0,
      pendingParam: null,
      pendingValue: '',
    }
  },

  computed: {
    tags() {
      if (!this.value) return []
      const result = []
      for (const [param, values] of Object.entries(this.value))
        for (const value of values)
          result.push({ param, value })
      return result
    },

    filteredParams() {
      if (!this.searchText)
        return this.params
      const q = this.searchText.toLowerCase()
      return this.params.filter(p => p.toLowerCase().includes(q))
    },
  },

  methods: {
    toggleDropdown() {
      this.showDropdown = !this.showDropdown
      this.searchText = ''
      this.highlightIndex = 0
      if (this.showDropdown)
        this.$nextTick(() => {
          if (this.$refs.searchInput)
            this.$refs.searchInput.focus()
        })
    },

    moveHighlight(delta) {
      const len = this.filteredParams.length
      if (len === 0) return
      this.highlightIndex = (this.highlightIndex + delta + len) % len
      this.$nextTick(() => {
        const el = this.$el.querySelector('.dropdown-item.highlighted')
        if (el)
          el.scrollIntoView({ block: 'nearest' })
      })
    },

    selectHighlighted() {
      if (this.filteredParams.length > 0)
        this.selectParam(this.filteredParams[this.highlightIndex])
    },

    selectParam(param) {
      this.showDropdown = false
      this.searchText = ''
      this.pendingParam = param
      this.pendingValue = ''
      this.$nextTick(() => {
        if (this.$refs.valueInput)
          this.$refs.valueInput.focus()
      })
    },

    commitFilter() {
      if (!this.pendingParam) return
      const val = this.pendingValue.trim()
      const filterVal = (val === '' || val === '*') ? null : val
      const args = {}
      // Copy existing filters.
      if (this.value)
        for (const [p, vs] of Object.entries(this.value))
          args[p] = [...vs]
      // Add the new filter value.
      if (args[this.pendingParam])
        args[this.pendingParam].push(filterVal)
      else
        args[this.pendingParam] = [filterVal]
      this.pendingParam = null
      this.pendingValue = ''
      this.$emit('input', args)
      this.$emit('change', args)
    },

    cancelPending() {
      this.pendingParam = null
      this.pendingValue = ''
    },

    clearAll() {
      this.$emit('input', null)
      this.$emit('change', null)
    },

    removeFilter(param, value) {
      const args = {}
      for (const [p, vs] of Object.entries(this.value || {}))
        if (p === param) {
          const filtered = vs.filter(v => v !== value)
          if (filtered.length > 0)
            args[p] = filtered
        }
        else
          args[p] = [...vs]
      const result = Object.keys(args).length > 0 ? args : null
      this.$emit('input', result)
      this.$emit('change', result)
    },

    onClickOutside(ev) {
      if (this.$refs.addFilter && !this.$refs.addFilter.contains(ev.target))
        this.showDropdown = false
    },
  },

  watch: {
    searchText() {
      this.highlightIndex = 0
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
@import 'src/styles/vars.scss';

.args-filter {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px;
  min-height: 28px;
}

.filter-tag {
  display: inline-flex;
  align-items: baseline;
  background: $global-select-background;
  border: 1px solid darken($apsis-frame-color, 5%);
  border-radius: 3px;
  padding: 1px 4px 1px 6px;
  font-size: 0.85em;
  line-height: 22px;
  max-width: 100%;
}

.tag-param {
  font-weight: 600;
  color: $apsis-job-color;
}

.tag-eq {
  color: $global-light-color;
  margin: 0 2px;
}

.tag-value {
  font-family: 'Roboto Mono', monospace;
  word-break: break-all;
}

.tag-remove {
  margin-left: 4px;
  cursor: pointer;
  color: $global-light-color;
  font-size: 1.1em;
  line-height: 1;
  padding: 0 2px;

  &:hover {
    color: #c44;
  }
}

.action-button {
  cursor: pointer;
  color: $global-light-color;
  font-size: 0.85em;
  border: 1px dashed $global-frame-color;
  border-radius: 3px;
  padding: 1px 8px;
  line-height: 22px;
  white-space: nowrap;

  &:hover {
    color: $apsis-job-color;
    border-color: darken($global-frame-color, 10%);
    background: $global-hover-background;
  }

  &.action-clear:hover {
    color: #c44;
  }
}

.add-filter {
  position: relative;
}

.dropdown {
  position: absolute;
  top: 100%;
  left: 0;
  z-index: 10;
  margin-top: 2px;
  background: white;
  border: 1px solid $global-frame-color;
  box-shadow: 4px 4px 4px #eee;
  min-width: 12em;
  max-height: 16em;
  overflow-y: auto;
}

.dropdown-search {
  width: calc(100% - 8px);
  margin: 4px;
  padding: 2px 6px;
  font-size: 0.85em;
  border: 1px solid $global-frame-color;
  line-height: 22px;
  box-sizing: border-box;

  &:focus {
    outline: none;
    border-color: $global-focus-color;
  }
}

.dropdown-item {
  padding: 4px 12px;
  cursor: pointer;
  white-space: nowrap;
  font-size: 0.9em;

  &:hover, &.highlighted {
    background: $global-select-background;
  }
}

.dropdown-empty {
  padding: 4px 12px;
  color: $global-light-color;
  font-size: 0.85em;
  font-style: italic;
}

.value-input {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 0.85em;
}

.pending-param {
  font-weight: 600;
  color: $apsis-job-color;
}

.pending-value {
  width: 10em;
  padding: 1px 6px;
  font-family: 'Roboto Mono', monospace;
  font-size: inherit;
  border: 1px solid $global-frame-color;
  line-height: 22px;

  &:focus {
    outline: none;
    border-color: $global-focus-color;
  }
}

.pending-ok, .pending-cancel {
  font-size: inherit;
  padding: 1px 8px;
  line-height: 22px;
  border: 1px solid $global-frame-color;
  border-radius: 3px;
  cursor: pointer;
  background: $global-background;

  &:hover {
    background: $global-hover-background;
  }
}
</style>
