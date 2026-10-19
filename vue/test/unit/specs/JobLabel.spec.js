import Vue from 'vue'
import JobLabel from '@/components/JobLabel'

function clicks(clickable) {
  const vm = new (Vue.extend(JobLabel))({ propsData: { label: 'x', clickable } }).$mount()
  const emitted = jest.fn()
  vm.$on('click', emitted)
  vm.$el.click()
  return emitted.mock.calls.length
}

test('a label only emits clicks when clickable', () => {
  expect(clicks(false)).toBe(0)
  expect(clicks(true)).toBe(1)
})

test('a label skips the click that ends a text selection', () => {
  const getSelection = jest.spyOn(window, 'getSelection').mockReturnValue({isCollapsed: false})
  expect(clicks(true)).toBe(0)
  getSelection.mockRestore()
})
