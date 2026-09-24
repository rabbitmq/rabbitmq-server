import { useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { enableFeatureFlag, featureFlagsQuery } from '../../api/resources/admin'
import type { FeatureFlag } from '../../api/types/admin'
import { errorMessage } from '../../api/errors'
import { useNotify } from '../../app/notifications'
import { useRefetchInterval } from '../../app/refresh'
import { safeHttpUrl } from '../../format/names'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'

export function FeatureFlagsPage() {
  const flags = useQuery({ ...featureFlagsQuery(), refetchInterval: useRefetchInterval('topology') })
  const queryClient = useQueryClient()
  const notify = useNotify()
  const [pending, setPending] = useState<ReadonlySet<string>>(new Set())
  const [experimental, setExperimental] = useState<FeatureFlag | undefined>()
  const [filter, setFilter] = useState('')

  const enable = async (names: string[]) => {
    setPending((current) => new Set([...current, ...names]))
    for (const name of names) {
      try {
        await enableFeatureFlag(name)
      } catch (err) {
        notify('error', `${name}: ${errorMessage(err)}`)
      }
    }
    setPending((current) => new Set([...current].filter((n) => !names.includes(n))))
    await queryClient.invalidateQueries({ queryKey: ['feature-flags'] })
  }

  const header = <PageHeader title="Feature Flags" documentTitle="Feature Flags" />
  if (!flags.data) return <>{header}{flags.error ? <ErrorMessage error={flags.error} what="feature flags" /> : <Loading />}</>
  // Required flags are always enabled and are not listed, as in the classic UI.
  const optional = flags.data.filter((f) => f.stability !== 'required')
  const disabledStable = optional.filter((f) => f.stability === 'stable' && f.state === 'disabled')
  const shown = optional.filter((f) => f.name.toLowerCase().includes(filter.toLowerCase()))

  return (
    <>
      {header}
      {disabledStable.length > 0 ? (
        <div className="callout callout-warn" data-testid="ff-disabled-stable-warning">
          <p style={{ marginTop: 0 }}>
            All stable feature flags must be enabled after completing an upgrade. Without enabling all flags, upgrading to future minor or major
            versions of RabbitMQ may not be possible. <a href="https://www.rabbitmq.com/docs/feature-flags">Learn more</a>
          </p>
          <button
            type="button"
            className="btn btn-primary"
            disabled={disabledStable.some((f) => pending.has(f.name))}
            onClick={() => void enable(disabledStable.map((f) => f.name))}
            data-testid="ff-enable-all"
          >
            Enable all stable feature flags
          </button>
        </div>
      ) : null}
      <Section id="feature-flags" title="Feature Flags">
        <div className="stack">
          <input type="search" placeholder="Filter" aria-label="Filter" value={filter} onChange={(e) => setFilter(e.target.value)} style={{ maxWidth: '20rem' }} />
          <div className="table-wrap">
            <table className="list" data-testid="feature-flags-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Specificities</th>
                  <th>State</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                {shown.map((flag) => {
                  const busy = pending.has(flag.name) || flag.state === 'state_changing'
                  return (
                    <tr key={flag.name}>
                      <td className="mono">{flag.name}</td>
                      <td>
                        {(flag as FeatureFlag & { callbacks?: string[] }).callbacks?.includes('enable') ? (
                          <abbr className="tag" title="This feature flag has a migration function which might take some time and consume resources.">
                            migration
                          </abbr>
                        ) : null}
                        {flag.stability === 'experimental' ? (
                          <abbr className="tag" style={{ color: 'var(--warn)' }} title="This is an experimental feature flag">
                            experimental
                          </abbr>
                        ) : null}
                        {flag.experiment_level === 'unsupported' ? (
                          <abbr className="tag" style={{ color: 'var(--bad)' }} title="Not yet supported; an upgrade path is not guaranteed">
                            unsupported
                          </abbr>
                        ) : null}
                      </td>
                      <td>
                        {flag.state === 'enabled' ? (
                          <span className="badge"><span className="dot dot-green" /> enabled</span>
                        ) : (
                          <button
                            type="button"
                            className="btn btn-small"
                            disabled={busy || flag.state === 'unavailable'}
                            onClick={() => (flag.stability === 'experimental' ? setExperimental(flag) : void enable([flag.name]))}
                            data-testid={`ff-enable-${flag.name}`}
                          >
                            {busy ? 'Enabling…' : flag.state === 'unavailable' ? 'Unavailable' : 'Enable'}
                          </button>
                        )}
                      </td>
                      <td>
                        {flag.desc}
                        {safeHttpUrl(flag.doc_url) ? (
                          <>
                            {' '}
                            <a href={safeHttpUrl(flag.doc_url)}>Learn more</a>
                          </>
                        ) : null}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      </Section>
      {experimental ? (
        <ExperimentalDialog
          flag={experimental}
          onCancel={() => setExperimental(undefined)}
          onConfirm={() => {
            void enable([experimental.name])
            setExperimental(undefined)
          }}
        />
      ) : null}
    </>
  )
}

function ExperimentalDialog({ flag, onCancel, onConfirm }: { flag: FeatureFlag; onCancel: () => void; onConfirm: () => void }) {
  const ref = useRef<HTMLDialogElement>(null)
  const unsupported = flag.experiment_level === 'unsupported'
  const [acks, setAcks] = useState([false, false])
  useEffect(() => {
    ref.current?.showModal()
  }, [])
  const ready = unsupported ? acks[0] && acks[1] : acks[0]
  return (
    <dialog ref={ref} onCancel={onCancel} style={{ maxWidth: '40rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }} data-testid="ff-experimental-dialog">
      <h2>Enabling an experimental feature flag</h2>
      <p>
        <strong>
          The <code>{flag.name}</code> feature flag is experimental.
        </strong>{' '}
        This means the functionality behind it is still a work in progress.
      </p>
      {unsupported ? (
        <>
          <p>
            The development of this feature is at an early stage. Support is not provided and enabling it in production is not recommended. Once it is
            enabled, upgrades to a future version of RabbitMQ are not guaranteed.
          </p>
          <label className="row">
            <input type="checkbox" checked={acks[0]} onChange={(e) => setAcks([e.target.checked, acks[1]])} /> I understand that support is not provided.
          </label>
          <label className="row">
            <input type="checkbox" checked={acks[1]} onChange={(e) => setAcks([acks[0], e.target.checked])} /> I understand that there is no guaranteed
            upgrade path.
          </label>
        </>
      ) : (
        <>
          <p>
            Try it in a test environment first. The feature flag is supported even though it is still experimental, so upgrades with it enabled are
            supported.
          </p>
          <label className="row">
            <input type="checkbox" checked={acks[0]} onChange={(e) => setAcks([e.target.checked, acks[1]])} /> I understand that this feature is
            experimental and should be tested first.
          </label>
        </>
      )}
      <div className="row" style={{ marginTop: '1rem' }}>
        <button type="button" className="btn btn-primary" disabled={!ready} onClick={onConfirm}>
          Enable {flag.name}
        </button>
        <button type="button" className="btn" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </dialog>
  )
}
