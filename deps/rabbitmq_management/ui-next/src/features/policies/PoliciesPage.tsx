import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { deletePolicy, policiesQuery, type PolicyKind } from '../../api/resources/admin'
import type { Policy } from '../../api/types/admin'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { useSelectedVhost } from '../../app/vhost'
import { isValidRegex } from '../../components/listState'
import { PolicyLink } from '../../components/Links'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'
import { fmtTableFlat } from '../../format/args'
import { PolicyForm } from './PolicyForm'

export function PoliciesPage() {
  const { access, overview } = useAppData()
  const canManageOperatorPolicies = access.isAdministrator && overview.is_op_policy_updating_enabled
  return (
    <>
      <PageHeader title="Policies" documentTitle="Policies" />
      <Section id="policies-user" title="User policies">
        <PolicyList kind="policies" linkNames={access.isPolicymaker} canDelete={false} />
      </Section>
      {access.isPolicymaker && access.canAccessVhosts ? (
        <Section id="policies-user-add" title="Add / update a policy" defaultOpen={false}>
          <PolicyForm kind="policies" />
        </Section>
      ) : null}
      <Section id="policies-operator" title="Operator policies">
        <PolicyList kind="operator-policies" linkNames={false} canDelete={canManageOperatorPolicies} />
      </Section>
      {canManageOperatorPolicies && access.canAccessVhosts ? (
        <Section id="policies-operator-add" title="Add / update an operator policy" defaultOpen={false}>
          <PolicyForm kind="operator-policies" />
        </Section>
      ) : null}
    </>
  )
}

/** Policy lists are not paginated by the server, so they are filtered here, as in the classic UI. */
function PolicyList({ kind, linkNames, canDelete }: { kind: PolicyKind; linkNames: boolean; canDelete: boolean }) {
  const { vhosts } = useAppData()
  const [vhost] = useSelectedVhost()
  const [filter, setFilter] = useState('')
  const [regex, setRegex] = useState(false)
  const policies = useQuery({ ...policiesQuery(kind, vhost), refetchInterval: useRefetchInterval('topology') })
  const remove = useApiMutation<Policy>({
    mutationFn: (p) => deletePolicy(kind, p.vhost, p.name),
    success: (_, p) => `Policy ${p.name} cleared`,
    invalidate: [[kind]],
  })
  if (!policies.data) return policies.error ? <ErrorMessage error={policies.error} what="policies" /> : <Loading />
  const matches = (name: string) => {
    if (filter === '') return true
    if (regex && isValidRegex(filter)) return new RegExp(filter, 'i').test(name)
    return name.toLowerCase().includes(filter.toLowerCase())
  }
  const shown = policies.data.filter((p) => matches(p.name))
  const showVhost = vhosts.length > 1 && vhost === ''
  return (
    <div className="stack">
      <div className="row">
        <input type="search" placeholder="Filter" aria-label="Filter" value={filter} onChange={(e) => setFilter(e.target.value)} />
        <label className="row" style={{ gap: '0.3rem' }}>
          <input type="checkbox" checked={regex} onChange={(e) => setRegex(e.target.checked)} /> Regex
        </label>
        <span className="muted">
          {shown.length} of {policies.data.length}
        </span>
      </div>
      {shown.length === 0 ? (
        <p className="muted">No policies</p>
      ) : (
        <div className="table-wrap">
          <table className="list" data-testid={`${kind}-table`}>
            <thead>
              <tr>
                {showVhost ? <th>Virtual host</th> : null}
                <th>Name</th>
                <th>Pattern</th>
                <th>Apply to</th>
                <th>Definition</th>
                <th className="num">Priority</th>
                {canDelete ? <th /> : null}
              </tr>
            </thead>
            <tbody>
              {shown.map((p) => (
                <tr key={`${p.vhost}|${p.name}`}>
                  {showVhost ? <td>{p.vhost}</td> : null}
                  <td>{linkNames ? <PolicyLink vhost={p.vhost} name={p.name} /> : p.name}</td>
                  <td className="mono">{p.pattern}</td>
                  <td>{p['apply-to']}</td>
                  <td>{fmtTableFlat(p.definition)}</td>
                  <td className="num">{p.priority}</td>
                  {canDelete ? (
                    <td>
                      <button
                        type="button"
                        className="btn btn-small btn-danger"
                        onClick={() => {
                          if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) remove.mutate(p)
                        }}
                      >
                        Clear
                      </button>
                    </td>
                  ) : null}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
