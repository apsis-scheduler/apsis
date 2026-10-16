import { Socket } from '@/websocket.js'

test('close passes the event through', () => {
  global.WebSocket = class {}
  const onClose = jest.fn()
  const socket = new Socket('ws://localhost/summary', null, null, null, onClose, {reconnect: false})
  socket.open()
  const event = {code: 4503}
  socket.websocket.onclose(event)
  expect(onClose).toHaveBeenCalledWith(event)
})
