<template lang="pug">
  span.job-label(
    :class="{ clickable, selected }"
    @click="clickable && $emit('click')"
  ) {{ label }}
</template>

<script>
export default {
  props: {
    label: String,
    clickable: {type: Boolean, default: false},
    selected: {type: Boolean, default: false},
  },
}
</script>

<style lang="scss">
@import '../styles/vars.scss';

span.job-label {
  margin-left: 4px;
  border: 1px solid #ddd;
  border-radius: 12px;
  background: #ccc;
  color: white;
  padding: 1px 8px;

  &.clickable {
    cursor: pointer;
  }

  &.selected {
    background: $global-focus-color;
  }
}

</style>
