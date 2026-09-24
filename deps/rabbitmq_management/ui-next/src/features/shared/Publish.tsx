import { useState, type FormEvent } from 'react'
import { publishMessage } from '../../api/resources/exchanges'
import type { AmqpTable } from '../../api/types/common'
import { ArgumentsEditor, rowsToTable, type ArgRow } from '../../components/ArgumentsEditor'
import { useNotify } from '../../app/notifications'
import { errorMessage } from '../../api/errors'
import { useApiMutation } from '../../components/mutation'

const PROPERTIES: [string, 'str' | 'int'][] = [
  ['content_type', 'str'],
  ['content_encoding', 'str'],
  ['correlation_id', 'str'],
  ['reply_to', 'str'],
  ['expiration', 'str'],
  ['message_id', 'str'],
  ['type', 'str'],
  ['user_id', 'str'],
  ['app_id', 'str'],
  ['cluster_id', 'str'],
  ['priority', 'int'],
  ['timestamp', 'int'],
]

type Target = { mode: 'queue'; vhost: string; queue: string; classic: boolean } | { mode: 'exchange'; vhost: string; exchange: string }

export function PublishForm(target: Target) {
  const notify = useNotify()
  const [routingKey, setRoutingKey] = useState('')
  const [deliveryMode, setDeliveryMode] = useState(target.mode === 'queue' && target.classic ? '1' : '2')
  const [headers, setHeaders] = useState<ArgRow[]>([])
  const [props, setProps] = useState<Record<string, string>>({})
  const [payload, setPayload] = useState('')
  const [encoding, setEncoding] = useState<'string' | 'base64'>('string')

  const publish = useApiMutation<AmqpTable, { routed: boolean }>({
    mutationFn: (properties) =>
      publishMessage(target.vhost, target.mode === 'queue' ? '' : target.exchange, {
        routing_key: target.mode === 'queue' ? target.queue : routingKey,
        payload,
        payload_encoding: encoding,
        properties,
      }),
    invalidate: [['queues'], ['exchanges']],
    onSuccess: ({ routed }) => {
      notify(routed ? 'success' : 'info', routed ? 'Message published.' : 'Message published, but not routed.')
    },
  })

  const submit = (event: FormEvent) => {
    event.preventDefault()
    const properties: AmqpTable = { delivery_mode: parseInt(deliveryMode, 10) }
    try {
      const headerTable = rowsToTable(headers)
      if (Object.keys(headerTable).length > 0) properties.headers = headerTable
    } catch (err) {
      notify('error', errorMessage(err))
      return
    }
    for (const [name, type] of PROPERTIES) {
      const value = props[name]
      if (value !== undefined && value !== '') properties[name] = type === 'int' ? parseInt(value, 10) : value
    }
    publish.mutate(properties)
  }

  return (
    <form className="form" onSubmit={submit} data-testid="publish-form">
      {target.mode === 'queue' ? (
        <p className="full">
          The message will be published to the default exchange with routing key <strong>{target.queue}</strong>, routing it to this queue.
        </p>
      ) : (
        <>
          <label htmlFor="publish-routing-key">Routing key</label>
          <input id="publish-routing-key" type="text" value={routingKey} onChange={(e) => setRoutingKey(e.target.value)} data-testid="publish-routing-key" />
        </>
      )}
      {target.mode === 'queue' && target.classic ? (
        <>
          <label htmlFor="publish-delivery-mode">Delivery mode</label>
          <select id="publish-delivery-mode" value={deliveryMode} onChange={(e) => setDeliveryMode(e.target.value)}>
            <option value="1">1 - Non-persistent</option>
            <option value="2">2 - Persistent</option>
          </select>
        </>
      ) : null}
      <span className="label">Headers</span>
      <ArgumentsEditor rows={headers} onChange={setHeaders} testId="publish-headers" />
      <span className="label">Properties</span>
      <div className="row">
        {PROPERTIES.map(([name]) => (
          <input
            key={name}
            type="text"
            aria-label={name}
            placeholder={name}
            value={props[name] ?? ''}
            onChange={(e) => setProps({ ...props, [name]: e.target.value })}
            style={{ width: '11rem' }}
          />
        ))}
      </div>
      <label htmlFor="publish-payload">Payload</label>
      <textarea id="publish-payload" value={payload} onChange={(e) => setPayload(e.target.value)} data-testid="publish-payload" />
      <label htmlFor="publish-encoding">Payload encoding</label>
      <select id="publish-encoding" value={encoding} onChange={(e) => setEncoding(e.target.value as 'string' | 'base64')}>
        <option value="string">String (default)</option>
        <option value="base64">Base64</option>
      </select>
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={publish.isPending} data-testid="publish-submit">
          Publish message
        </button>
      </div>
    </form>
  )
}
