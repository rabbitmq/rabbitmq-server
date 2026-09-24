import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { deprecatedFeaturesQuery } from '../../api/resources/admin'
import { useRefetchInterval } from '../../app/refresh'
import { safeHttpUrl } from '../../format/names'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'

const PHASES: Record<string, string> = {
  permitted_by_default: 'Permitted by default',
  denied_by_default: 'Denied by default',
  disconnect: 'Disconnect',
  removed: 'Removed',
}

export function DeprecatedFeaturesPage() {
  const interval = useRefetchInterval('topology')
  const all = useQuery({ ...deprecatedFeaturesQuery(false), refetchInterval: interval })
  const used = useQuery({ ...deprecatedFeaturesQuery(true), refetchInterval: interval })
  const [filter, setFilter] = useState('')
  const usedNames = new Set((used.data ?? []).map((f) => f.name))
  const header = <PageHeader title="Deprecated Features" documentTitle="Deprecated Features" />
  if (!all.data) return <>{header}{all.error ? <ErrorMessage error={all.error} what="deprecated features" /> : <Loading />}</>
  const shown = all.data.filter((f) => f.name.toLowerCase().includes(filter.toLowerCase()))
  return (
    <>
      {header}
      {usedNames.size > 0 ? (
        <div className="callout callout-warn" data-testid="deprecated-in-use">
          Deprecated features are being used: {[...usedNames].join(', ')}. While using deprecated features, upgrading to future minor or major
          versions of RabbitMQ may not be possible.
        </div>
      ) : null}
      <Section id="deprecated-features" title="All deprecated features">
        <div className="stack">
          <input type="search" placeholder="Filter" aria-label="Filter" value={filter} onChange={(e) => setFilter(e.target.value)} style={{ maxWidth: '20rem' }} />
          <div className="table-wrap">
            <table className="list" data-testid="deprecated-features-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Deprecation phase</th>
                  <th>Current configuration</th>
                  <th>In use</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                {shown.map((f) => (
                  <tr key={f.name}>
                    <td className="mono">{f.name}</td>
                    <td>{PHASES[f.deprecation_phase] ?? f.deprecation_phase}</td>
                    <td>{f.state}</td>
                    <td>{usedNames.has(f.name) ? <span style={{ color: 'var(--warn)' }}>yes</span> : 'no'}</td>
                    <td>
                      {f.desc}
                      {safeHttpUrl(f.doc_url) ? (
                        <>
                          {' '}
                          <a href={safeHttpUrl(f.doc_url)}>Learn more</a>
                        </>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </Section>
    </>
  )
}
