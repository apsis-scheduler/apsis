import Vue from 'vue'
import JobLabel from '@/components/JobLabel'

test('a label only emits clicks when clickable', () => {
  const clicks = clickable => {
    const vm = new (Vue.extend(JobLabel))({ propsData: { label: 'x', clickable } }).$mount()
    const emitted = jest.fn()
    vm.$on('click', emitted)
    vm.$el.click()
    return emitted.mock.calls.length
  }
  expect(clicks(false)).toBe(0)
  expect(clicks(true)).toBe(1)
})
