/**
 * Apsis API client
 */

function getUrl(...path) {
  const url = new URL(location)
  url.pathname = '/api/v1/' + path.join('/')
  return url
}

// WebSocket scheme must match the page protocol: wss on HTTPS, ws otherwise. 
const WS_PROTOCOL = location.protocol === 'https:' ? 'wss' : 'ws' 

export function getDependenciesUrl(run_id) {
  return getUrl('runs', run_id, 'dependencies')
}

export function getMarkUrl(run_id, state) {
  return getUrl('runs', run_id, 'mark', state)
}

export function getOutputDataUrl(run_id, output_id) {
  return getUrl('runs', run_id, 'output', output_id)
}

export function getOutputDataUpdatesUrl(run_id, output_id, start) {
  const url = getUrl('runs', run_id, 'output', output_id, 'updates')
  url.protocol = WS_PROTOCOL
  if (start !== undefined)
    url.search = '?start=' + start
  return url
}

export function getOutputUrl(run_id) {
  return getUrl('runs', run_id, 'outputs')
}

export function getRerunUrl(run_id) {
  return getUrl('runs', run_id, 'rerun')
}

export function getRunUpdatesUrl(run_id, init) {
  const url = getUrl('runs', run_id, 'updates')
  url.protocol = WS_PROTOCOL
  if (init)
    url.search = '?init'
  return url
}

export function getRunUrl(run_id) {
  return getUrl('runs', run_id)
}

export function getSummaryUrl(init) {
  const url = getUrl('summary')
  url.protocol = WS_PROTOCOL
  if (init)
    url.search = '?init'
  return url
}

export function getStopUrl(run_id) {
  return getUrl('runs', run_id, 'stop')
}

export function getSignalUrl(run_id, signame) {
  return getUrl('runs', run_id, 'signal', signame)
}

export function getSkipUrl(run_id) {
  return getUrl('runs', run_id, 'skip')
}

export function getStartUrl(run_id) {
  return getUrl('runs', run_id, 'start')
}

export function getSubmitRunUrl() {
  return getUrl('runs')
}

export function getSubmitRunBody(job_id, args, time) {
  return {
    job_id,
    args,
    times: {
      schedule: time,
    },
  }
}

export function getUrlForOperation(operation, run_id) {
  switch (operation) {
    case 'skip': return getSkipUrl(run_id)
    case 'start': return getStartUrl(run_id)
    case 'rerun': return getRerunUrl(run_id)
    case 'stop': return getStopUrl(run_id)
    case 'terminate': return getSignalUrl(run_id, 'SIGTERM')
    case 'kill': return getSignalUrl(run_id, 'SIGKILL')
    default:
      if (operation.startsWith('mark ')) return getMarkUrl(run_id, operation.substring(5))
  }
  console.error('unknown operation:', operation)
  return ''
}
