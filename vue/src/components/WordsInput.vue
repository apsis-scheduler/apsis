<template lang="pug">
  input(
    v-model="text"
    @change="onChange"
    @keyup.escape="text = ''; onChange()"
  )
</template>

<script>
import { isEqual } from 'lodash'

export default { 
  name: 'WordsInput',
  props: {
    value: {type: Array, default: null},
    // splits the text into words
    separator: {type: String, default: ' '},
  },

  data() {
    return {
      text: this.join(this.value),
    }
  },

  methods: {
    join(value) {
      return value ? value.join(this.separator === ' ' ? ' ' : this.separator + ' ') : ''
    },

    onChange() {
      // Emit 'input' and 'change' only if the value is effectively different.
      const words = this.text.split(this.separator).map(w => w.trim()).filter(w => w)
      const value = words.length > 0 ? words : null
      if (!isEqual(value, this.value)) {
        this.$emit('input', value)
        this.$emit('change', value)
      }
    },
  },

  watch: {
    value(value) {
      this.text = this.join(value)
    },
  },
}
</script>

<style lang="scss" scoped>
input {
  width: 100%;
}
</style>
