<template lang="pug">
router-link.run-link(
  :to="{ name: 'run', params: { run_id: runId } }"
) {{ runId }}

</template>

<script>
export default {
  props: ['runId'],
}
</script>

<style lang="scss">
@import '../styles/vars.scss';

.run-link {
  background-color: white;
  color: $apsis-run-color;
  box-sizing: border-box;
  border: 2px solid #e4f4ec;
  border-radius: 0;
  padding: 0 0.4rem;
  font-size: 0.875rem;
  align-items: center;
  justify-content: center;
  cursor: default;
  display: inline-block;
  width: 10ch;

  &:hover {
    color: $apsis-run-color;
    border-color: #a0d0c8;
    text-decoration: none;
  }
}

</style>
