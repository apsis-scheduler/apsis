 <template lang="pug">
div.jobs-list
  div.controls
    div
      .label Job Path:
      PathNav(
        :path="path"
        @path="$emit('path', $event)"
      )

    div
      .label Keywords:
      WordsInput(
        :value="keywords"
        @input="$emit('keywords', $event)"
        @words="liveKeywords = $event"
      )
      HelpButton
        p Syntax: <b>keyword keyword&hellip;</b>
        p Show only jobs whose ID contains each <b>keyword</b>.

    div
      .label Labels:
      WordsInput(
        :value="labels"
        @input="$emit('labels', $event)"
      )
      HelpButton
        p Syntax: <b>label label&hellip;</b>
        p Show only jobs with each <b>label</b>.

    div.buttons(style="grid: span 3")
      button(
        @click="expandAll(true)"
      ) Expand All
      button(
        @click="expandAll(false)"
      ) Collapse All
      button(
        @click="$emit('showRuns')"
      ) Show Runs

  div.jobs-header
    div Job
    div Parameters
    div Description

  DynamicScroller.jobs-scroller(
    ref="scroller"
    :items="jobRows"
    key-field="key"
    :min-item-size="31"
    :style="{ height: 'calc(100vh - ' + scrollerTop + 'px - 1rem)' }"
  )
    template(slot-scope="{ item, active }")
      DynamicScrollerItem.row(
        :item="item"
        :active="active"
        :size-dependencies="[item.job]"
      )
        div(:style="{ 'padding-left': (30 * item.subpath.length) + 'px' }")

          //- a job
          div.cell(
            v-if="item.job"
          )
            svg.icon(viewBox="0 0 1800 1800", xmlns="http://www.w3.org/2000/svg")
              path(d="M 200 600 L 1600 600 L 1600 1600 L 200 1600 L 200 600" stroke="#666" stroke-width="100" fill="transparent")
              path(d="M 200 600 L 350 1000 L 1450 1000 L 1600 600" stroke="#888" stroke-width="80" fill="transparent")
              path(d="M 500 600 a 250 200 0 0 1 800 0" stroke="#888" stroke-width="150" fill="transparent")

            Job.name(:job-id="item.job.job_id" :name="item.name")

            JobLabel(
              v-for="label in item.job.metadata.labels"
              :key="label"
              :label="label"
            )

          //- a dir entry
          div.cell(
            v-else
            v-on:click="toggleExpand(item.path)"
          )
            TriangleIcon.indent.icon(
              v-if="isExpanded(item.path)"
              direction="down"
            )
            TriangleIcon.indent.icon(
              v-else
              direction="right"
            )

            svg.icon(viewBox="0 0 1800 1800", xmlns="http://www.w3.org/2000/svg")
              path(d="M 200 300 L 700 300 L 800 500 L 1600 500 L 1600 1600 L 200 1600 L 200 300" stroke="#666" stroke-width="100" fill="#f2f6f4")

            a.dir(v-on:click="$emit('path', item.path.join('/'))") {{ item.name }}

        div
          span.params(v-if="item.job && item.job.params.length > 0") {{ join(item.job.params, ', ') }}

        div.description(v-html="item.job && descriptionExcerptHtml(item.job.metadata.description || '')")

</template>

<script>
import { filter, join, sortBy } from 'lodash'
import { DynamicScroller, DynamicScrollerItem } from 'vue-virtual-scroller'

import HelpButton from '@/components/HelpButton'
import Job from '@/components/Job'
import JobLabel from '@/components/JobLabel'
import { descriptionExcerptHtml } from '@/markdown'
import PathNav from '@/components/PathNav'
import Program from '@/components/Program'
import { matchKeywords, includesAll } from '@/runs'
import SearchInput from '@/components/SearchInput'
import store from '@/store.js'
import TriangleIcon from '@/components/icons/TriangleIcon'
import WordsInput from '@/components/WordsInput'

/**
 * Arranges an array of jobs into a tree by job ID path components.
 * 
 * Each node in the tree is [subtrees, jobs], where subtrees maps
 * names to subnodes, and jobs maps names to jobs.
 * 
 * @returns the root node
 */
function jobsToTree(jobs) {
  const tree = [{}, {}]
  for (const job of jobs) {
    const parts = job.job_id.split('/')
    const name = parts.splice(parts.length - 1, 1)[0]

    var subtree = tree
    for (const part of parts)
      subtree = subtree[0][part] = subtree[0][part] || [{}, {}]

    subtree[1][name] = job
  }

  return tree
}

/**
 * Flattens a tree into items for rendering.
 * 
 * Generates [path, subpath, name, job] items, where job is null
 * for directory items.  
 * 
 * @param basePath - path corresponding to `tree`, as array of parts
 * @param tree - the tree node to flatten
 * @param expanded - lookup of expanded tree paths
 */
function* flattenTree(basePath, tree, expanded, path = []) {
  const [subtrees, items] = tree

  for (const [name, subtree] of sortBy(Object.entries(subtrees))) {
    const dirPath = basePath.concat(path, [name])
    yield [dirPath, path, name, null]
    if (expanded[dirPath])
      yield* flattenTree(basePath, subtree, expanded, path.concat([name]))
  }

  for (const [name, item] of sortBy(Object.entries(items)))
    yield [basePath.concat(path, [name]), path, name, item]
}

export default {
  props: {
    path: {type: String, default: null},
    keywords: {type: Array, default: null},
    labels: {type: Array, default: null},

    // FIXME: Delete.
    query: {type: String, default: ''},
  },

  data() {
    return {
      liveKeywords: this.keywords,
      scrollerTop: 0,
      store,
    }
  },

  mounted() {
    // size the scroller to fill the rest of the window
    this.scrollerTop = this.$refs.scroller.$el.getBoundingClientRect().top + window.scrollY
  },

  components: {
    DynamicScroller,
    DynamicScrollerItem,
    HelpButton,
    Job,
    JobLabel,
    PathNav,
    Program,
    SearchInput,
    TriangleIcon,
    WordsInput,
  },

  computed: {
    expand() {
      return this.store.state.jobsExpand
    },

    /** Jobs after applying the filter.  */
    visibleJobs() {
      var jobs = Array.from(store.state.jobs.values())
      if (this.path) {
        const path = this.path
        const prefix = path + '/'
        jobs = filter(jobs, job => job.job_id === this.path || job.job_id.startsWith(prefix))
      }
      if (this.labels)
        jobs = filter(jobs, job => includesAll(this.labels, job.metadata.labels))
      if (this.liveKeywords) {
        const keywords = this.liveKeywords.map(s => s.toLowerCase())
        jobs = filter(jobs, job => matchKeywords(keywords, job.job_id.toLowerCase()))
      }

      return jobs
    },

    /** Filtered jobs, as a tree.  */
    jobsTree() {
      return jobsToTree(this.visibleJobs)
    },

    /** Filtered jobs subtree for current path.  */
    jobsDir() {
      var tree = this.jobsTree
      if (this.path)
        for (const part of this.path.split('/'))
          tree = tree[0][part] || [{}, {}]
      return tree
    },

    /** Filtered jobs subtree flattened to display rows, including dirs.  */
    jobRows() {
      const parts = this.path ? this.path.split('/') : []
      return Array.from(
        flattenTree(parts, this.jobsDir, this.expand),
        ([path, subpath, name, job]) => ({
          // trailing slash keeps a dir key distinct from a job of the same name
          key: path.join('/') + (job ? '' : '/'),
          path,
          subpath,
          name,
          job,
        }),
      )
    },

  },

  methods: {
    descriptionExcerptHtml,

    join,

    toggleExpand(path) {
      this.$set(this.expand, path, !this.expand[path])
    },

    isExpanded(path) {
      return this.expand[path]
    },

    expandAll(expanded) {
      let expand = {}
      if (expanded)
        for (const job of store.state.jobs.values()) {
          const path = job.job_id.split('/')
          path.pop()
          while (path.length > 0) {
            expand[path] = true
            path.pop()
          }
        }
      this.store.state.jobsExpand = expand
    },
  },
}
</script>

<style lang="scss" scoped>
@import '@/styles/index.scss';
@import '@/styles/vars.scss';

.controls {
  width: 80em;
  margin-top: 1em;
  background: $global-control-background;
  padding: 12px 18px;

  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px 2em;
  justify-items: left;
  align-items: baseline;

  white-space: nowrap;
  line-height: 28px;

  input {
    background-color: $global-background;
  }

  > input {
    width: 100%;
  }

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

  .buttons {
    height: 30px;
    grid-column: span 3;
    display: block;

    button {
      height: 100%;
      margin-right: 8px;
    }
  }
}

.jobs-list {
  min-width: 100%;
  width: min-content;
}

.jobs-header, .row {
  display: grid;
  grid-template-columns: 600px 250px minmax(600px, 1fr);
  align-items: center;

  > div {
    padding: 3px 12px 4px 12px;

    &:last-child {
      padding-right: 2px;
    }
  }
}

.jobs-header {
  border-bottom: 1px solid #aaa;
  color: #676;
  font-weight: 600;
  font-size: 90%;
  text-transform: uppercase;

  > div {
    padding-top: 12px;
    padding-bottom: 0;
    line-height: 30px;

    &:first-child {
      padding-left: 2px;
    }
  }
}

.row {
  border-bottom: 1px solid $apsis-grid-color;
  border-radius: 3px;

  &:hover {
    background-color: $global-hover-background;
  }

  span.indent {
    width: 36px;
  }

  .icon {
    width: 18px;
    margin-right: 4px;
  }

  div.cell {
    min-height: 30px;
    display: inline-flex;
    flex-direction: row;
    flex-wrap: wrap;
    align-items: center;
    gap: 0 4px;
  }

  .job-title {
    .name {
      font-weight: 500;
    }
    .params {
      padding-left: 0.2rem;
      span {
        padding: 0 0.2rem;
      }
    }
  }

  .description {
    font-size: 85%;
    color: $global-light-color;

    p {
      margin: 0;
    }
  }

  .schedule {
    font-size: 85%;
    padding-top: 4px;
    ul {
      margin: 0;
    }

    .disabled {
      color: #aaa;
    }
  }

  a.dir {
    cursor: default;

    &:hover {
      text-decoration: underline;
    }
  }
}
</style>
