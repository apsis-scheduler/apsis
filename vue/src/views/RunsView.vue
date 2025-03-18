<template lang="pug">
div
  h1 Runs

  //- The table of runs.
  RunsList(
    :query="query"
    @query="onQueryChange"
    :timeControls="true"
  )

</template>

<script>
import { isEqual } from 'lodash'
import { argsToArray, arrayToArgs } from '@/runs'
import RunsList from '@/components/RunsList'
import { formatCompactUTCTime, parseCompactUTCTime } from '@/time'

/**
 * Strips off all occurrences of `suffix` at the end of `string`.
 */
function rstrip(string, suffix) {
  const len = suffix.length
  while (len <= string.length && string.slice(-len) === suffix)
    string = string.slice(0, -len)
  return string
}

export default {
  name: 'RunsView',
  components: {
    RunsList,
  },

  data() {
    return {
      query: this.urlToQuery(this.$route.query),
    }
  },

  watch: {
    // When the route changes due to the browser's forward/back buttons,
    // trigger an update to the query.
    '$route'(to) {
      this.$set(this, 'query', this.urlToQuery(to.query))
    }
  },

  methods: {
    /**
     * Converts the query part of a URL to a query object for RunsList.
     */
    urlToQuery(url) {
      const splitWords = (param) => param ? param.split(',') : null
      return {
        path: url.path ? rstrip(url.path, '/') : null,
        keywords: splitWords(url.keywords),
        labels: splitWords(url.labels),
        states: splitWords(url.states),
        args: url.args ? arrayToArgs(splitWords(url.args)) : null,
        grouping: url.ungroup !== null,
        show: url.show ? parseInt(url.show) : 50,
        time: url.time ? parseCompactUTCTime(url.time) : 'now',
        asc: url.asc === 'true',
      }
    },

    /**
     * Renders the query object from RunsList as the query part of a URL.
     */
    queryToUrl(query) {
      const url = {}
      function set(param, val) {
        if (val === undefined)
          ;
        else if (val === null)
          url[param] = null
        else
          url[param] = val.toString().trim()
      }

      const joinWords = (words) => words !== null ? words.join(',') : undefined
      set('path', query.path || undefined)
      set('keywords', joinWords(query.keywords))
      set('labels', joinWords(query.labels))
      set('states', joinWords(query.states))
      set('args', joinWords(query.args === null ? null : argsToArray(query.args)))
      set('ungroup', query.grouping ? undefined : null)
      set('show', query.show === 50 ? undefined : query.show)
      set('time', query.time === 'now' ? undefined : formatCompactUTCTime(query.time))
      set('asc', query.asc ? 'true' : undefined)
      return url
    },

    onQueryChange(query) {
      // Convert the RunsList query to URL query.  If anything has changed,
      // push this to the router to update the browser URL and push an element
      // onto the undo stack.
      const url = this.queryToUrl(query)
      if (!isEqual(this.$route.query, url))
        this.$router.push({ query: url })
    },

    onShowJobs() {
      this.$router.push({
        name: 'jobs-list',
        params: {
          path: this.path || undefined,
        },
      })
    },
  },
}
</script>

<style lang="scss">
h3 {
  margin: 0;
  margin-bottom: 1ex;
}

// FIXME: Elsewhere
.flex-margin {
  display: flex;
  gap: 16px;
}
</style>

