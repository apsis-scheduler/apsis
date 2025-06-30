/*
 * WebSocket connection that receives JSON messages.
 */
export class Socket {
  constructor(url, onMessage, onConnect, onErr, onClose, { reconnect = true }) {
    this.url        = url
    this.websocket  = null
    this.onMessage  = onMessage
    this.onConnect  = onConnect
    this.onErr      = onErr
    this.onClose    = onClose
    this.reconnect  = reconnect
    this.isOpen     = false
  }

  _connect() {
    if (!this.isOpen)
      return

    if (this.websocket !== null)
      // Already have a websocket.
      return

    console.log('websocket connecting:', this.url.toString())
    this.websocket = new WebSocket(this.url)

    this.websocket.onopen = () => {
      this.onConnect()
    }

    this.websocket.onerror = (event) => {
      console.log('websocket error:', event)
      this.onErr(event)
      this.websocket.close()
    }

    this.websocket.onmessage = this.onMessage

    this.websocket.onclose = () => {
      console.log('websocket closed:', this.url.toString())
      this.onClose()
      this.websocket = null
      // Retry the connection after a second.
      if (this.isOpen && this.reconnect)
        setTimeout(() => this._connect(), 1000)
    }
  }

  open() {
    this.isOpen = true
    this._connect()
  }

  close() {
    if (this.isOpen) {
      if (this.websocket !== null)
        this.websocket.close()
      this.isOpen = false
    }
  }
}

/*
 * Parses `arr` in the style of an HTTP request or response, without a request
 * or status line, i.e. headers, a blank line, and payload.
 *
 * @param {ArrayBuffer} the request or response
 * @return a header object and a `Uint8Array` payload
 */
export function parseHttp(arr) {
  arr = new Uint8Array(arr)
  let start = 0
  let header = {}
  const decoder = new TextDecoder('ascii')

  for (let i = 0; i < arr.length - 4; ++i)
    // Look for \r\n.
    if (arr[i] === 13 && arr[i + 1] === 10)
      if (i === start)
        // Second newline in a row.  The rest is body.
        return [header, arr.slice(i + 2)]

      else {
        // Got a headerline.
        const line = decoder.decode(arr.slice(start, i))
        // Split at colon.
        const c = line.indexOf(':')
        if (c === -1) {
          console.log('invalid header line:', line)
          return [null, null]
        }
        const name = line.slice(0, c)
        const value = line.slice(c + 1).trim()
        header[name.toLowerCase()] = value
        start = i + 2
      }

  // Failed to split.
  return [null, null]
}

