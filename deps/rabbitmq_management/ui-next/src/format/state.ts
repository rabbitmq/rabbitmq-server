export type StateColour = 'green' | 'grey' | 'yellow' | 'red'

export interface DisplayState {
  colour: StateColour
  text: string
  explanation?: string
}

interface StatefulObject {
  state?: string
  idle_since?: string
  terminated_by?: string
}

const EXPLANATIONS: Record<string, [StateColour, string]> = {
  blocked: ['red', 'Resource alarm: connection blocked.'],
  blocking: ['yellow', 'Resource alarm: connection will block on publish.'],
  flow: ['yellow', 'Publishing rate recently throttled by server.'],
  down: ['red', 'The queue is located on a cluster node or nodes that are down.'],
  crashed: ['red', 'The queue has crashed repeatedly and been unable to restart.'],
  stopped: ['red', 'The queue process was stopped by the vhost supervisor.'],
  minority: ['yellow', 'The queue does not have sufficient online members to make progress.'],
  timeout: ['yellow', 'The queue leader did not respond to its status request.'],
}

/** The state of a queue, connection or channel, as `fmt_object_state` shows it. */
export function objectState(obj: StatefulObject): DisplayState | undefined {
  if (obj.state === undefined) return undefined
  if (obj.idle_since !== undefined) {
    return { colour: 'grey', text: 'idle', explanation: `Idle since ${obj.idle_since}` }
  }
  if (obj.state === 'terminated') {
    const by = obj.terminated_by ? ` by "${obj.terminated_by}"` : ''
    return { colour: 'yellow', text: obj.state, explanation: `The queue is being deleted${by}.` }
  }
  const known = EXPLANATIONS[obj.state]
  if (known) return { colour: known[0], text: obj.state, explanation: known[1] }
  return { colour: 'green', text: obj.state }
}

/** The state of a vhost across the cluster, as `fmt_vhost_state` shows it. */
export function vhostState(clusterState: Record<string, string> | undefined): DisplayState {
  const states = Object.entries(clusterState ?? {})
  const down = states.filter(([, state]) => state === 'stopped' || state === 'nodedown').map(([node]) => node)
  const running = states.filter(([, state]) => state === 'running').length
  if (down.length === 0) return { colour: 'green', text: 'running' }
  if (running === 0) return { colour: 'red', text: 'stopped', explanation: 'Vhost supervisor is not running.' }
  return {
    colour: 'yellow',
    text: 'partial',
    explanation: `Vhost supervisor is stopped on some cluster nodes: ${down.join(', ')}`,
  }
}
