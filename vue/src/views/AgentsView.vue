<template lang="pug">
div
  div
    div.header
      h1 Groups
      div
        input(type="checkbox" v-model="showDisconnected")
        label Show disconnected

    div.groups
      div.group(v-for="(conns, group_id) in filteredGroups" :key="group_id")
        div.name()
          div.group_id {{ group_id }}
          div.count {{ conns.filter(c => c.shutdown_state === 'active').length }} active connections
          div.count {{ conns.length }} connections

        div.conns
          div.conn(v-for="conn in conns" :class="{ connected: conn.info.stats.connected }")
            div.basics
              label State
              span(:class="['shutdown-' + conn.shutdown_state]") {{ conn.shutdown_state }}
              label Host
              span {{ conn.info.proc.hostname }}
              label User &amp; Group
              span {{ conn.info.proc.username }}:{{ conn.info.proc.groupname }}
              label PID
              span {{ conn.info.proc.pid }}
              label Connection ID
              span {{ conn.info.conn.conn_id }}
              label Restricted Exe
              span
                tt {{ conn.info.conn.restricted_exe }}
            div.stats
              template(v-for="(value, name) in conn.info.stats")
                label {{ name.replaceAll('_', ' ') }}
                div {{ value }}

</template>

<script>
import { filter, mapValues } from 'lodash'

import store from '@/store.js'
import { JOBS_SCOPE, setSummaryScope } from '@/summary.js'

export default {
  props: [],

  data() {
    return {
      store,
      timerId: undefined,
      showDisconnected: true,
    }
  },

  created() {
    setSummaryScope(JOBS_SCOPE)
  },

  computed: {
    filteredGroups() {
      let conns = store.state.agentConns.values()
      if (!this.showDisconnected)
        // Hide disconnected connections.
        conns = filter(conns, c => c.info.stats.connected)
      const groups = {}
      for (let conn of conns) {
        const group_id = conn.info.conn.group_id
        const group = groups[group_id]
        if (group)
          group.push(conn)
        else
          groups[group_id] = [conn]
      }
      // Sort connections in each group by hostname.
      mapValues(groups, g => g.sort(c => c.info.proc.hostname))
      return groups
    },
  },
}
</script>

<style lang="scss" scoped>
@import 'src/styles/vars.scss';

$gray: #e8e8e8;

.header {
  margin-bottom: 32px;

  display: flex;
  align-items: center;
  column-gap: 24px;

  h1 {
    flex-grow: 1;
  }

  button {
    height: 28px;
  }
}

groups {
  font-size: 100%;
}

.group_id {
  font-size: 120%;
  font-weight: bold;
}

.groups {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  row-gap: 24px;

  .group {
    border: 1px solid $gray;
    border-radius: 4px;

    display: grid;
    grid-template-columns: 24ex 1fr;
  }

  .name {
    padding: 8px 16px;
    background: $gray;
  }
}

.conns {
  display: flex;
  flex-direction: column;

  .conn {
    &:not(.connected) {
      color: #a0a0a0;
    }

    border-left: 1px solid $gray;
    border-bottom: 1px solid $gray;
    &:last-child {
      border-bottom: none;
    }

    display: grid;
    grid-template-columns: 1fr 1fr;
    white-space: nowrap;

    > * {
      padding: 8px 16px;
    }

    .basics {
      align-self: start;

      display: grid;
      grid-template-columns: 16ex 1fr;
      row-gap: 4px;

      label {
        font-weight: bold;
      }
    }

    .shutdown-active {
      color: green;
    }
    .shutdown-idling {
      color: orange;
    }
    .shutdown-done {
      color: red;
    }

    .stats {
      font-size: 85%;
      display: grid;
      grid-template-columns: 28ex 1fr;

      font-family: $base-code-font-family;
    }

    .bold {
      font-weight: bold;
    }
  }
}
</style>
