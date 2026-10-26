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

  computed: {
    words() {
      const words = this.text.split(this.separator).map(w => w.trim()).filter(w => w)
      return words.length > 0 ? words : null
    },
  },

  methods: {
    join(value) {
      return value ? value.join(this.separator === ' ' ? ' ' : this.separator + ' ') : ''
    },

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
