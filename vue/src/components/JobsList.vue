 <template lang="pug">
div
  div.controls
    div
      .label Job Path:
      PathNav(
        :path="path"
        :suggestions="allJobPaths"
        @path="$emit('path', $event)"
      )

    div
      .label Keywords:
      WordsInput(
        :value="keywords"
        @input="$emit('keywords', $event)"
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

  table.widetable
    colgroup
      col(style="max-width: 300px;")
      col(style="max-width: 300px;")
      col(style="")

    thead
      tr
        th
          div.cell
            | Job

        th Parameters
        th Description

    tbody
      tr.grid(
        v-for="[path, subpath, name, job] in jobRows"
        :key="subpath.concat([name]).join('/')"
      )
        td(:style="{ 'padding-left': (30 * subpath.length) + 'px' }")

          //- a job
          div.cell(
            v-if="job" 
          )
            svg.icon(viewBox="0 0 1800 1800", xmlns="http://www.w3.org/2000/svg")
              path(d="M 200 600 L 1600 600 L 1600 1600 L 200 1600 L 200 600" stroke="#666" stroke-width="100" fill="transparent")
              path(d="M 200 600 L 350 1000 L 1450 1000 L 1600 600" stroke="#888" stroke-width="80" fill="transparent")
              path(d="M 500 600 a 250 200 0 0 1 800 0" stroke="#888" stroke-width="150" fill="transparent")

            Job.name(:job-id="job.job_id" :name="name")

            JobLabel(
              v-for="label in job.metadata.labels"
              :key="label"
              :label="label"
            )

          //- a dir entry
          div.cell(
            v-else 
            v-on:click="toggleExpand(path)"
          )
            TriangleIcon.indent.icon(
              v-if="isExpanded(path)"
              direction="down"
            )
            TriangleIcon.indent.icon(
              v-else
              direction="right"
            )

            svg.icon(viewBox="0 0 1800 1800", xmlns="http://www.w3.org/2000/svg")
              path(d="M 200 300 L 700 300 L 800 500 L 1600 500 L 1600 1600 L 200 1600 L 200 300" stroke="#666" stroke-width="100" fill="#f2f6f4")

            a.dir(v-on:click="$emit('path', path.join('/'))") {{ name }}

        td.params
          span.params(v-if="job && job.params.length > 0") {{ join(job.params, ', ') }}

        td.description
          div(v-html="job && markdown(exerptDescription(job.metadata.description || ''))")

</template>

<script>
import { filter, join, sortBy } from 'lodash'
import showdown from 'showdown'

import HelpButton from '@/components/HelpButton'
import Job from '@/components/Job'
import JobLabel from '@/components/JobLabel'
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
      store,
    }
  },

  components: {
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

    allJobPaths() {
      const paths = new Set()
      for (const job of store.state.jobs.values()) {
        const parts = job.job_id.split('/')
        // Add directory prefixes only, not the full job ID.
        for (let i = 1; i < parts.length; i++)
          paths.add(parts.slice(0, i).join('/'))
      }
      return Array.from(paths).sort()
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
      if (this.keywords) {
        const keywords = this.keywords.map(s => s.toLowerCase())
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
      return Array.from(flattenTree(parts, this.jobsDir, this.expand))
    },

  },

  methods: {
    // Returns a shortened form of the description Markdown `src`.
    exerptDescription(src) {
      var paragraph = src.split('\n\n')[0]
      if (paragraph.length > 256)
        paragraph = paragraph.substring(0, 256) + '…'
      return paragraph
    },

    // Converts Markdown `src` to HTML.
    markdown(src) { return src.trim() === '' ? '' : (new showdown.Converter()).makeHtml(src) },

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

table {
  span.indent {
    width: 36px;
  }

  th {
    text-align: left;
  }

  .icon {
    width: 18px;
    margin-right: 4px;
  }

  div.cell {
    height: 30px;
    display: inline-flex;
    flex-direction: row;
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
