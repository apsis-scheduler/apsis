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
import { isEqual, isUndefined, omitBy } from 'lodash'

import JobsList from '@/components/JobsList'

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

  methods: {
    pushRoute() {
      const joinWords = (words) => words !== null ? words.join(',') : undefined
      const location = {
        params: {
          path: toPathParts(this.path),
        },
        query: omitBy({
          keywords: joinWords(this.keywords),
          labels: joinWords(this.labels),
        }, isUndefined),
      }
      // syncing from a route change lands here too, so skip pushing the same url
      const route = this.$router.resolve(location).route
      if (route.path !== this.$route.path || !isEqual(route.query, this.$route.query))
        this.$router.push(location)
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
