import Vue from 'vue'
import WordsInput from '@/components/WordsInput'

function mount(propsData) {
  return new (Vue.extend(WordsInput))({propsData}).$mount()
}

describe('WordsInput', () => {
  test('splits on spaces by default', () => {
    const vm = mount({value: null})
    const emitted = []
    vm.$on('input', v => emitted.push(v))
    vm.text = ' a  b '
    vm.onChange()
    expect(emitted).toEqual([['a', 'b']])
  })

  test('splits on a separator, trims words, and joins them back', () => {
    const vm = mount({value: ['critical', 'eqidx no-ops board'], separator: ','})
    expect(vm.text).toBe('critical, eqidx no-ops board')
    const emitted = []
    vm.$on('input', v => emitted.push(v))
    vm.text = 'vor no-ops-board ,, critical'
    vm.onChange()
    expect(emitted).toEqual([['vor no-ops-board', 'critical']])
  })
})
