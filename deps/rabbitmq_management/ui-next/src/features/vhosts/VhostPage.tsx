import { useQuery } from '@tanstack/react-query'
import { useNavigate, useParams } from '@tanstack/react-router'
import { deleteVhost, permissionsQuery, vhostQuery } from '../../api/resources/admin'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { ChartRangeSelect, useChartRange } from '../../charts/ChartRangeSelect'
import { dataRateSeries, messageRateSeries, queueLengthSeries } from '../../charts/stats'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { VhostState } from '../../components/State'
import { ErrorMessage, Loading } from '../../components/Status'
import { Bool, Facts } from '../../components/Values'
import { ChartBlock } from '../shared/ChartBlock'
import { PermissionsPanel, TopicPermissionsPanel } from '../shared/Permissions'
import { RestartVhost } from './VhostsPage'

export function VhostPage() {
  const { name } = useParams({ from: '/vhosts/$name' })
  const { stats } = useAppData()
  const range = useChartRange()
  const vhost = useQuery({ ...vhostQuery(name, range), refetchInterval: useRefetchInterval('stats') })
  const permissions = useQuery(permissionsQuery({ vhost: name }))
  const header = <PageHeader kind="Virtual host" title={name} documentTitle={`Virtual host ${name}`} meta={vhost.data ? <VhostState clusterState={vhost.data.cluster_state} /> : null} />
  if (!vhost.data) return <>{header}{vhost.error ? <ErrorMessage error={vhost.error} what={`Virtual host ${name}`} /> : <Loading />}</>
  const v = vhost.data
  return (
    <>
      {header}
      {permissions.data?.length === 0 ? (
        <div className="callout callout-warn">No users have permission to access this virtual host. Use "Set permission" below to grant access.</div>
      ) : null}
      {!stats.disabled ? (
        <Section id="vhost-overview" title="Overview" actions={<ChartRangeSelect />}>
          <div className="stack">
            <div className="grid-2">
              <ChartBlock title="Queued messages" series={queueLengthSeries(v)} kind="gauge" />
              {stats.hasRates && v.message_stats ? <ChartBlock title="Message rates" series={messageRateSeries(v.message_stats)} kind="rate" /> : null}
              <ChartBlock title="Data rates" series={dataRateSeries(v)} kind="rate" bytes />
            </div>
            <div>
              <h3>Details</h3>
              <Facts
                rows={[
                  ['Description', v.description],
                  ['Tags', (v.tags ?? []).join(', ')],
                  ['Tracing enabled', <Bool value={v.tracing} key="t" />],
                  ['Default queue type', v.default_queue_type && v.default_queue_type !== 'undefined' ? v.default_queue_type : '<not set>'],
                  ['Deletion protection', v.protected_from_deletion ? 'enabled' : 'disabled'],
                  [
                    'State',
                    <table className="facts" key="s">
                      <tbody>
                        {Object.entries(v.cluster_state ?? {}).map(([node, state]) => (
                          <tr key={node}>
                            <th>{node}</th>
                            <td>
                              {state} {state === 'stopped' ? <RestartVhost vhost={v.name} node={node} /> : null}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>,
                  ],
                ]}
              />
            </div>
          </div>
        </Section>
      ) : null}
      <Section id="vhost-permissions" title="Permissions">
        <PermissionsPanel scope={{ vhost: name }} />
      </Section>
      <Section id="vhost-topic-permissions" title="Topic permissions">
        <TopicPermissionsPanel scope={{ vhost: name }} />
      </Section>
      <Section id="vhost-delete" title="Delete this vhost" defaultOpen={false}>
        <DeleteVhost name={name} />
      </Section>
    </>
  )
}

function DeleteVhost({ name }: { name: string }) {
  const navigate = useNavigate()
  const remove = useApiMutation({
    mutationFn: () => deleteVhost(name),
    success: `Virtual host ${name} deleted`,
    invalidate: [['vhosts'], ['permissions']],
    onSuccess: () => navigate({ to: '/vhosts' }),
  })
  return (
    <button
      type="button"
      className="btn btn-danger"
      disabled={remove.isPending}
      onClick={() => {
        if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) remove.mutate()
      }}
      data-testid="delete-vhost"
    >
      Delete this virtual host
    </button>
  )
}
