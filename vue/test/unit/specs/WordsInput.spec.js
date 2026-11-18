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
    const vm = mount({value: ['critical', 'needs review'], separator: ','})
    expect(vm.text).toBe('critical, needs review')
    const emitted = []
    vm.$on('input', v => emitted.push(v))
    vm.text = 'needs review ,, critical'
    vm.onChange()
    expect(emitted).toEqual([['needs review', 'critical']])
  })
})
