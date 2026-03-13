<template lang="pug">
.tags-filter
  .filter-tag(
    v-for="(tag, i) in value || []"
    :key="tag + '-' + i"
  )
    span.tag-value {{ tag }}
    span.tag-remove(@click="removeTag(tag)") &times;

  .add-filter(ref="addFilter")
    span.action-button(@click="toggleDropdown")
      | + {{ label }}
    .dropdown(v-if="showDropdown")
      input.dropdown-search(
        ref="searchInput"
        v-model="searchText"
        :placeholder="'type to search...'"
        @keyup.escape="showDropdown = false"
        @keydown.enter.prevent="selectHighlighted"
        @keydown.down.prevent="moveHighlight(1)"
        @keydown.up.prevent="moveHighlight(-1)"
      )
      .dropdown-item(
        v-for="(item, i) in filteredSuggestions"
        :key="item"
        :class="{ highlighted: i === highlightIndex }"
        @mousedown.prevent="addTag(item)"
        @mouseenter="highlightIndex = i"
      ) {{ item }}
      .dropdown-empty(v-if="filteredSuggestions.length === 0") No matching {{ label }}s
  span.action-button.action-clear(v-if="value && value.length" @click="clearAll") &times; clear
  slot
</template>

<script>
export default {
  name: 'TagsFilter',

  props: {
    // Current filter values: array of strings, or null.
    value: { type: Array, default: null },
    // Known suggestions for the dropdown.
    suggestions: { type: Array, default: () => [] },
    // Label for the add button (e.g. "label", "keyword").
    label: { type: String, default: 'tag' },
  },

  data() {
    return {
      showDropdown: false,
      searchText: '',
      highlightIndex: 0,
    }
  },

  computed: {
    filteredSuggestions() {
      const active = new Set(this.value || [])
      const available = this.suggestions.filter(s => !active.has(s))
      if (!this.searchText)
        return available
      const q = this.searchText.toLowerCase()
      return available.filter(s => s.toLowerCase().includes(q))
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
      const hasCustom = this.searchText && !this.filteredSuggestions.includes(this.searchText)
      const len = this.filteredSuggestions.length + (hasCustom ? 1 : 0)
      if (len === 0) return
      this.highlightIndex = (this.highlightIndex + delta + len) % len
      this.$nextTick(() => {
        const el = this.$el.querySelector('.dropdown-item.highlighted')
        if (el)
          el.scrollIntoView({ block: 'nearest' })
      })
    },

    selectHighlighted() {
      if (this.highlightIndex < this.filteredSuggestions.length)
        this.addTag(this.filteredSuggestions[this.highlightIndex])
      else if (this.searchText)
        this.addTag(this.searchText)
    },

    addTag(tag) {
      const trimmed = tag.trim()
      if (!trimmed) return
      const current = this.value || []
      if (current.includes(trimmed)) {
        this.showDropdown = false
        this.searchText = ''
        return
      }
      const result = [...current, trimmed]
      this.showDropdown = false
      this.searchText = ''
      this.$emit('input', result)
      this.$emit('change', result)
    },

    removeTag(tag) {
      const result = (this.value || []).filter(t => t !== tag)
      this.$emit('input', result.length > 0 ? result : null)
      this.$emit('change', result.length > 0 ? result : null)
    },

    clearAll() {
      this.$emit('input', null)
      this.$emit('change', null)
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

.tags-filter {
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

.dropdown-custom {
  font-style: italic;
  color: $global-light-color;
}

.dropdown-empty {
  padding: 4px 12px;
  color: $global-light-color;
  font-size: 0.85em;
  font-style: italic;
}
</style>
