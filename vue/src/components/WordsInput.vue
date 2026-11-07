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
  },

  data() {
    return {
      text: this.value ? this.value.join(' ') : '',
    }
  },

  computed: {
    words() {
      return this.text ? this.text.split(' ').filter(w => w) : null
    },
  },

  methods: {
    onChange() {
      // Emit 'input' and 'change' only if the value is effectively different.
      if (!isEqual(this.words, this.value)) {
        this.$emit('input', this.words)
        this.$emit('change', this.words)
      }
    },
  },

  watch: {
    // emit 'words' as the text changes, before it's committed
    words(words) { this.$emit('words', words) },

    value(value) {
      this.text = this.value ? this.value.join(' ') : ''
    },
  },
}
</script>

<style lang="scss" scoped>
input {
  width: 100%;
}
</style>
