import router from '@/router'

jest.mock('@/websocket.js', () => ({
  Socket: class { open() {} close() {} },
}))

test('list pages set the title and other pages leave it to the view', async () => {
  for (const [path, title] of [['/agents', 'agents'], ['/jobs', 'jobs'], ['/runs', 'runs']]) {
    await router.push(path)
    expect(document.title).toEqual(title + ' - apsis')
  }
  document.title = 'r1 - apsis'
  await router.push('/runs/r1')
  expect(document.title).toEqual('r1 - apsis')
})
