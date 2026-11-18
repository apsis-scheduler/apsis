<template lang="pug">
  .app
    navbar.navbar
    ErrorToast
    router-view.view
</template>

<script>
import ErrorToast from '@/components/ErrorToast'
import navbar from '@/components/navbar'
import store from '@/store.js'

export default {
  name: 'App',
  components: {
    ErrorToast,
    navbar,
  },

  data() {
    return {
      store,
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

