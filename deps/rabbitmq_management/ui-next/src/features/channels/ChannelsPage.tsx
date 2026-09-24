import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { channelListQuery } from '../../api/resources/connections'
import { nodesQuery } from '../../api/resources/overview'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { useSelectedVhost } from '../../app/vhost'
import { ColumnChooser } from '../../components/ColumnChooser'
import { DataTable } from '../../components/DataTable'
import { ListControls } from '../../components/ListControls'
import { useListState } from '../../components/listState'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage } from '../../components/Status'
import { channelColumns } from './channelColumns'

export function ChannelsPage() {
  const { stats, access, vhosts } = useAppData()
  const [vhost] = useSelectedVhost()
  const list = useListState('channels')
  const nodes = useQuery({ ...nodesQuery(), enabled: access.isMonitoring, staleTime: 60_000 })
  const columns = channelColumns(stats, { showVhost: vhosts.length > 1 && vhost === '', showNode: (nodes.data?.length ?? 0) > 1 })
  const channels = useQuery({ ...channelListQuery(vhost, list.params), refetchInterval: useRefetchInterval('stats'), placeholderData: keepPreviousData })
  return (
    <>
      <PageHeader title="Channels" documentTitle="Channels" />
      <Section id="channels-list" title="All channels" actions={<ColumnChooser mode="channels" columns={columns} />}>
        <ListControls state={list} page={channels.data} noun="channels" error={channels.error} />
        {channels.error ? <ErrorMessage error={channels.error} what="channels" /> : null}
        <DataTable
          mode="channels"
          columns={columns}
          rows={channels.data?.items ?? []}
          rowKey={(c) => c.name}
          sort={list.params}
          onSortChange={(sort) => list.update({ ...sort, page: 1 })}
          empty={channels.isPending ? 'Loading…' : 'No channels'}
        />
      </Section>
    </>
  )
}
