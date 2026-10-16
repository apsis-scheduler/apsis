<template lang="pug">
span.run-args
  span.run-arg(
    v-for="[param, arg] in pairs"
    :class="{ clickable, selected: selected !== null && selected[param] === arg }"
    @click="clickable && $emit('click', param, arg)"
  )
    span.param {{ param }}
    span.eq =
    span.arg {{ arg }}
</template>

<script>
import { toPairs } from 'lodash'

export default {
  props: {
    args: Object,
    clickable: {type: Boolean, default: false},
    // args filter whose matching args are highlighted
    selected: {type: Object, default: null},
  },

  computed: {
    pairs() {
      return toPairs(this.args)
    },
  }
}
</script>

<style lang="scss">
@import '../styles/vars.scss';

span.run-args {
  span.run-arg {
    & + .run-arg {
      margin-left: 6px;
    }
    display: inline-block;

    &:hover {
      text-decoration: underline;
    }

    &.clickable {
      cursor: pointer;
    }

    &.selected {
      background: $global-select-background;
    }

    .param {
      color: #aaa;
    }
    .eq {
      color: #aaa;
    }
    .arg {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }
}
</style>
