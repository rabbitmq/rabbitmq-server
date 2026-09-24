import { useQuery } from '@tanstack/react-query'
import { useNavigate, useParams } from '@tanstack/react-router'
import { deleteExchange, exchangeBindingsQuery, exchangeQuery } from '../../api/resources/exchanges'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { ChartRangeSelect, useChartRange } from '../../charts/ChartRangeSelect'
import { messageRateSeries } from '../../charts/stats'
import { PolicyLink } from '../../components/Links'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'
import { AmqpTableView, Facts, FeatureTags } from '../../components/Values'
import { exchangeName } from '../../format/names'
import { AddBindingForm, BindingsTable } from '../shared/Bindings'
import { ChartBlock } from '../shared/ChartBlock'
import { PublishesTable } from '../shared/MessageStatsTables'
import { PublishForm } from '../shared/Publish'

export function ExchangePage() {
  const { vhost, name: routeName } = useParams({ from: '/exchanges/$vhost/$name' })
  const { stats } = useAppData()
  const range = useChartRange()
  const exchange = useQuery({ ...exchangeQuery(vhost, routeName, range), refetchInterval: useRefetchInterval('stats') })
  // The default exchange is addressed as amq.default, but its name is the empty string.
  const name = exchange.data?.name ?? (routeName === 'amq.default' ? '' : routeName)

  const header = (
    <PageHeader
      kind="Exchange"
      title={exchangeName(name)}
      documentTitle={`Exchange ${exchangeName(name)}`}
      meta={
        <>
          <span>
            Virtual host <strong>{vhost}</strong>
          </span>
          {exchange.data ? <span className="tag">{exchange.data.type}</span> : null}
        </>
      }
    />
  )
  if (!exchange.data) {
    return (
      <>
        {header}
        {exchange.error ? <ErrorMessage error={exchange.error} what={`Exchange ${exchangeName(name)}`} /> : <Loading />}
      </>
    )
  }
  const x = exchange.data
  return (
    <>
      {header}
      {exchange.error ? <ErrorMessage error={exchange.error} what="the latest exchange state" /> : null}
      <Section id="exchange-overview" title="Overview" actions={stats.hasRates ? <ChartRangeSelect /> : null}>
        <div className="grid-2">
          {stats.hasRates ? <ChartBlock title="Message rates" series={messageRateSeries(x.message_stats)} kind="rate" testId="chart-exchange-rates" /> : null}
          <div>
            <h3>Details</h3>
            <Facts
              rows={[
                ['Type', x.type],
                ['Features', <FeatureTags obj={x} key="f" />],
                ['Policy', x.policy ? <PolicyLink vhost={x.vhost} name={x.policy} /> : null],
                ...(Object.keys(x.arguments ?? {}).length > 0 ? ([['Arguments', <AmqpTableView table={x.arguments} key="a" />]] as [string, React.ReactNode][]) : []),
              ]}
            />
          </div>
        </div>
      </Section>

      {stats.ratesMode === 'detailed' && !stats.disabled ? (
        <Section id="exchange-rates-breakdown" title="Message rates breakdown" defaultOpen={false}>
          <div className="grid-2">
            <PublishesTable rows={x.incoming} by="channel" label="Incoming" />
            <PublishesTable rows={x.outgoing} by="queue" label="Outgoing" />
          </div>
        </Section>
      ) : null}

      <Section id="exchange-bindings" title="Bindings" defaultOpen={false}>
        {name === '' ? (
          <p>
            The default exchange is implicitly bound to every queue, with a routing key equal to the queue name. It is not possible to explicitly
            bind to, or unbind from the default exchange. It also cannot be deleted.
          </p>
        ) : (
          <ExchangeBindings vhost={vhost} name={name} />
        )}
      </Section>

      {!x.internal ? (
        <Section id="exchange-publish" title="Publish message" defaultOpen={false}>
          <PublishForm mode="exchange" vhost={vhost} exchange={name} />
        </Section>
      ) : null}

      {name !== '' ? (
        <Section id="exchange-delete" title="Delete this exchange" defaultOpen={false}>
          <DeleteExchange vhost={vhost} name={name} />
        </Section>
      ) : null}
    </>
  )
}

function ExchangeBindings({ vhost, name }: { vhost: string; name: string }) {
  const interval = useRefetchInterval('topology')
  const destination = useQuery({ ...exchangeBindingsQuery(vhost, name, 'destination'), refetchInterval: interval })
  const source = useQuery({ ...exchangeBindingsQuery(vhost, name, 'source'), refetchInterval: interval })
  return (
    <div className="stack">
      {destination.error ? <ErrorMessage error={destination.error} what="bindings" /> : null}
      {destination.data && destination.data.length > 0 ? (
        <>
          <BindingsTable bindings={destination.data} mode="exchange_destination" />
          <p className="muted">⇓</p>
        </>
      ) : null}
      <p>
        <strong>This exchange</strong>
      </p>
      <p className="muted">⇓</p>
      {source.error ? <ErrorMessage error={source.error} what="bindings" /> : null}
      {source.data ? <BindingsTable bindings={source.data} mode="exchange_source" /> : <Loading />}
      <AddBindingForm vhost={vhost} name={name} mode="exchange" />
    </div>
  )
}

function DeleteExchange({ vhost, name }: { vhost: string; name: string }) {
  const navigate = useNavigate()
  const remove = useApiMutation({
    mutationFn: () => deleteExchange(vhost, name),
    success: `Exchange ${name} deleted`,
    invalidate: [['exchanges']],
    onSuccess: () => navigate({ to: '/exchanges' }),
  })
  return (
    <button
      type="button"
      className="btn btn-danger"
      disabled={remove.isPending}
      onClick={() => {
        if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) remove.mutate()
      }}
      data-testid="delete-exchange"
    >
      Delete
    </button>
  )
}
