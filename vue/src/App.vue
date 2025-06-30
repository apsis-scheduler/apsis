<template lang="pug">
  .app
    navbar.navbar
    ErrorToast
    router-view.view
</template>

<script>
import * as api from '@/api'
import ErrorToast from '@/components/ErrorToast'
import navbar from '@/components/navbar'
import { Socket } from '@/websocket.js'
import store from '@/store.js'
import { processMsgs, clearRunState } from '@/runs.js'

export default {
  name: 'App',
  components: {
    ErrorToast,
    navbar,
  },

  data() {
    return {
      summarySocket: null,
      store,
    }
  },

  created() {
    const store = this.store

    this.summarySocket = new Socket(
      api.getSummaryUrl(true),
      // onMessage
      msg => processMsgs(JSON.parse(msg.data), store.state),
      // onConnect
      () => {
        // Clear state on connect; the server will send all runs and jobs.
        clearRunState(store.state)
        store.state.errors.pop('connection error')
        store.state.errors.pop('websocket closed: refresh the page')
      },
      // onError
      this.showToastError,
      // onClose
      () => {
        // warn the user that the socket is closed so that they can refresh the page
        // because we don't automatically reconnect
        store.state.errors.push('websocket closed: refresh the page')
      },
      // don't reconnect to avoid bug where browser can't keep up, apsis closes the
      // connection due to backpressure, then the browser continues the cycle by
      // requesting the full run summary history
      { reconnect: false },
    )
    this.summarySocket.open()
  },

  destroyed() {
    this.summarySocket.close()
  },

  methods: {
    showToastError(event) {
      store.state.errors.push('connection error')
    }
  },

}
</script>

<style lang="scss" scoped>
.app {
  max-width: none;
  margin-left: auto;
  margin-right: auto;
  font-family: "Roboto", Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
}

.view {
  margin-top: 1.5rem;
  max-width: none;
  padding-left: 40px;
  padding-right: 40px;
}
</style>

