import { useQuery } from '@tanstack/react-query'
import { useNavigate, useParams } from '@tanstack/react-router'
import { deletePolicy, policyQuery } from '../../api/resources/admin'
import { useAccess } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'
import { AmqpTableView, Facts } from '../../components/Values'
import { PolicyForm } from './PolicyForm'

export function PolicyPage() {
  const { vhost, name } = useParams({ from: '/policies/$vhost/$name' })
  const access = useAccess()
  const navigate = useNavigate()
  const policy = useQuery({ ...policyQuery('policies', vhost, name), refetchInterval: useRefetchInterval('topology') })
  const remove = useApiMutation({
    mutationFn: () => deletePolicy('policies', vhost, name),
    success: `Policy ${name} deleted`,
    invalidate: [['policies']],
    onSuccess: () => navigate({ to: '/policies' }),
  })
  const header = <PageHeader kind="Policy" title={name} documentTitle={`Policy ${name}`} meta={<span>Virtual host <strong>{vhost}</strong></span>} />
  if (!policy.data) return <>{header}{policy.error ? <ErrorMessage error={policy.error} what={`Policy ${name}`} /> : <Loading />}</>
  const p = policy.data
  return (
    <>
      {header}
      <Section id="policy-overview" title="Overview">
        <Facts
          rows={[
            ['Pattern', <code key="p">{p.pattern}</code>],
            ['Apply to', p['apply-to']],
            ['Definition', <AmqpTableView table={p.definition} key="d" />],
            ['Priority', p.priority],
          ]}
        />
      </Section>
      {access.isPolicymaker ? (
        <>
          <Section id="policy-update" title="Update this policy" defaultOpen={false}>
            <PolicyForm kind="policies" initial={p} />
          </Section>
          <Section id="policy-delete" title="Delete this policy" defaultOpen={false}>
            <button
              type="button"
              className="btn btn-danger"
              disabled={remove.isPending}
              onClick={() => {
                if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) remove.mutate()
              }}
              data-testid="delete-policy"
            >
              Delete this policy
            </button>
          </Section>
        </>
      ) : null}
    </>
  )
}
