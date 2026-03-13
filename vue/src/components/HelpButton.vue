<template lang="pug">
  .base(
    @mouseenter="show = true"
    @mouseleave="show = false"
  )
    div
      HelpIcon(
        :color="show ? '#b37' : '#d8d8d8'"
      ).icon
    .popup(v-if="show")
      slot

</template>

<script>
import HelpIcon from '@/components/icons/HelpIcon'

export default {
  components: {
    HelpIcon,
  },

  data() {
    return {
      show: false,
    }
  }
}
</script>

<style lang="scss" scoped>
@import 'src/styles/vars.scss';

.base {
  display: inline-block;
  vertical-align: middle;
}

.icon {
  width: 1em;
  height: 1em;
}

.popup {
  position: absolute;
  z-index: 1;
  max-width: 34em;
  text-align: left;
  white-space: normal;

  background-color: #f4f4f4;
  border: 3px solid #b37;
  border-radius: 8px;
  padding: 12px 20px;

  font-size: 90%;
  line-height: normal;
}
</style>
