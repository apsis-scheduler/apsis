<template lang="pug">
span.path-nav
  span(v-if="this.parts.length > 0")
    a.home.dirnav(@click="$emit('path', '')")
      HomeIcon(
    )

    span(v-for="[subdir, name] in prefixes")
      a.part.dirnav(@click="$emit('path', subdir)") {{ name }}
      |  /
    span.part.clickable(ref="trigger" @click="toggleDropdown") {{ last }} &#9662;

  span.placeholder(v-else @click="toggleDropdown" ref="trigger") all jobs &#9662;

  .dropdown-anchor(v-if="showDropdown" ref="dropdown")
    .dropdown
      input.dropdown-search(
        ref="searchInput"
        v-model="searchText"
        placeholder="type to filter..."
        @keyup.escape="showDropdown = false"
        @keydown.enter.prevent="selectHighlighted"
        @keydown.down.prevent="moveHighlight(1)"
        @keydown.up.prevent="moveHighlight(-1)"
      )
      .dropdown-item(
        v-for="(path, i) in filteredSuggestions"
        :key="path"
        :class="{ highlighted: i === highlightIndex }"
        @mousedown.prevent="selectPath(path)"
        @mouseenter="highlightIndex = i"
      ) {{ path }}
      .dropdown-empty(v-if="filteredSuggestions.length === 0") No matching paths

</template>

<script>
import HomeIcon from '@/components/icons/HomeIcon'

export default {
  name: 'PathNav',
  props: {
    path: { type: String },
    suggestions: { type: Array, default: () => [] },
  },
  components: {
    HomeIcon,
  },

  data() {
    return {
      showDropdown: false,
      searchText: '',
      highlightIndex: 0,
    }
  },

  computed: {
    parts() {
      return this.path ? this.path.split('/') : []
    },

    prefixes() {
      const prefixes = []
      for (var i = 0; i < this.parts.length - 1; ++i)
        prefixes.push([this.parts.slice(0, i + 1).join('/'), this.parts[i]])
      return prefixes
    },

    last() {
      return this.parts[this.parts.length - 1]
    },

    childSuggestions() {
      if (!this.path)
        return this.suggestions
      const prefix = this.path + '/'
      return this.suggestions.filter(s => s.startsWith(prefix))
    },

    filteredSuggestions() {
      if (!this.searchText)
        return this.childSuggestions
      const q = this.searchText.toLowerCase()
      return this.childSuggestions.filter(s => s.toLowerCase().includes(q))
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
      const len = this.filteredSuggestions.length
      if (len === 0) return
      this.highlightIndex = (this.highlightIndex + delta + len) % len
      this.$nextTick(() => {
        const el = this.$el.querySelector('.dropdown-item.highlighted')
        if (el)
          el.scrollIntoView({ block: 'nearest' })
      })
    },

    selectHighlighted() {
      if (this.filteredSuggestions.length > 0)
        this.selectPath(this.filteredSuggestions[this.highlightIndex])
    },

    selectPath(path) {
      this.showDropdown = false
      this.searchText = ''
      this.$emit('path', path)
    },

    onClickOutside(ev) {
      if (this.$refs.dropdown && !this.$refs.dropdown.contains(ev.target)
        && this.$refs.trigger && !this.$refs.trigger.contains(ev.target))
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
@import '../styles/vars.scss';

.path-nav {
  position: relative;
}

.home {
  margin-right: 8px;
  &.dirnav:hover {
    border: 1px dotted $global-light-color;
  }
}

.part {
  cursor: default;

  &.clickable {
    cursor: pointer;
    &:hover {
      color: $apsis-job-color;
      text-decoration: underline;
    }
  }
}

.dirnav {
  border: 1px solid transparent;
  &:hover {
    text-decoration: underline;
  }
}

.placeholder {
  color: #888;
  cursor: pointer;

  &:hover {
    color: $apsis-job-color;
    text-decoration: underline;
  }
}

.dropdown-anchor {
  position: absolute;
  top: 100%;
  left: 0;
  z-index: 10;
}

.dropdown {
  margin-top: 2px;
  background: white;
  border: 1px solid $global-frame-color;
  box-shadow: 4px 4px 4px #eee;
  min-width: 16em;
  max-height: 20em;
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
</style>
