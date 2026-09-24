import { objectState, vhostState, type DisplayState } from '../format/state'

export function StateBadge({ state }: { state: DisplayState | undefined }) {
  if (!state) return null
  return (
    <span className="badge" title={state.explanation} data-testid="state">
      <span className={`dot dot-${state.colour}`} aria-hidden="true" />
      {state.text}
    </span>
  )
}

export function ObjectState({ obj }: { obj: Parameters<typeof objectState>[0] }) {
  return <StateBadge state={objectState(obj)} />
}

export function VhostState({ clusterState }: { clusterState: Record<string, string> | undefined }) {
  return <StateBadge state={vhostState(clusterState)} />
}
