<template lang="pug">
nav
  svg(
    width="5em"
    height="4em"
    version="1.1"
    xmlns="http://www.w3.org/2000/svg"
  )
    circle(cx="1.5em" cy="2.05em" r="20%" stroke="black" fill="transparent" stroke-width="3.1")
    circle(cx="1.5em" cy="1.68em" r="4%" fill="#40c060")

  .title
    | APSİS

  router-link.link(to="/jobs") Jobs
  router-link.link(to="/runs") Runs
  router-link.link(to="/agents") Agents

  .spacer(style="flex-basis: 100%")

  .item(v-if="user") {{ user }}
  clock.item 
  TimeZoneSelect.tz.item

</template>

<script>
import * as api from '../api'
import Clock from './Clock.vue'
import TimeZoneSelect from './TimeZoneSelect'

export default {
  name: 'navbar',
  components: {
    Clock,
    TimeZoneSelect,
  },

  data() {
    return {
      user: null,
    }
  },

  created() {
    fetch(api.getWhoamiUrl())
      .then(async (response) => {
        if (response.ok)
          this.user = (await response.json()).user
      })
  },
}
</script>

<style lang="scss" scoped>
nav {
  background: #eee;
  height: 48px;
  padding: 4px 40px;
  display: flex;
  align-content: space-between;
  align-items: center;
  column-gap: 16px;
}

.title {
  font-family: Helvetica;
  font-size: 150%;
  font-weight: 700;
  padding-left: 0;
  letter-spacing: .1rem;
  margin-right: 12px;
  padding-top: 4px;
  color: #222;
}

.link {
  color: #333;
  text-transform: uppercase;
  border: 1px solid transparent;
  padding: 0 8px 2px 8px;
  &:hover {
    text-decoration: none;
    border: 1px solid #ccc;
  }
}

.clock {
  letter-spacing: .04rem;
}
</style>

