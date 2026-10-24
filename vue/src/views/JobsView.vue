<template lang="pug">
div
  h1 Jobs

  JobsList(
    :path="path"
    @path="path = $event"
    :keywords="keywords"
    @keywords="keywords = $event"
    :labels="labels"
    @labels="labels = $event"
    @showRuns="showRuns"
  )

</template>

<script>
import JobsList from '@/components/JobsList'
import { JOBS_SCOPE, setSummaryScope } from '@/summary.js'

function toPathStr(path) {
  return (
    !path ? ''
    : Array.isArray(path) ? path.join('/')
    : path
  )
}

function toPathParts(path) {
  return (
    !path ? []
    : Array.isArray(path) ? path
    : path.split('/')
  )
}

export default {
  name: 'JobsView',
  props: {
  },

  components: {
    JobsList,
  },

  data() {
    const labels = this.$route.query.labels
    const keywords = this.$route.query.keywords

    return {
      keywords: keywords ? keywords.split(',') : null,
      labels: labels ? labels.split(',') : null,
      path: toPathStr(this.$route.params.path),
    }
  },

  created() {
    setSummaryScope(JOBS_SCOPE)
  },

  methods: {
    pushRoute() {
      const joinWords = (words) => words !== null ? words.join(',') : undefined
      this.$router.push({
        params: {
          path: toPathParts(this.path),
        },
        query: {
          keywords: joinWords(this.keywords),
          labels: joinWords(this.labels),
        }
      })
    },

    showRuns() {
      this.$router.push({
        name: 'runs-list',
        query: {
          path: this.path,
          keywords: this.keywords ? this.keywords.join(',') : undefined,
          labels: this.labels ? this.labels.join(',') : undefined,
        },
      })
    },
  },

  watch: {
    '$route'(to) {
      const keywords = this.$route.query.keywords
      const labels = this.$route.query.labels
      console.log('$route to', keywords, labels)

      this.keywords = keywords ? keywords.split(',') : null
      this.labels = labels ? labels.split(',') : null
      this.path = toPathStr(this.$route.params.path)
    },

    path() { this.pushRoute() },
    keywords() { this.pushRoute() },
    labels() { this.pushRoute() },
  },
}
</script>

<style lang="scss" scoped>
</style>
