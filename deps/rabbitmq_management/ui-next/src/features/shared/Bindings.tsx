import { useState, type FormEvent } from 'react'
import { addBinding, deleteBinding } from '../../api/resources/queues'
import type { Binding } from '../../api/types/queues'
import { ArgumentsEditor, rowsToTable, type ArgRow } from '../../components/ArgumentsEditor'
import { ExchangeLink, QueueLink } from '../../components/Links'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { useNotify } from '../../app/notifications'
import { fmtTableFlat } from '../../format/args'
import { errorMessage } from '../../api/errors'

/** The classic UI truncates binding lists at the same length. */
const MAX_BINDINGS = 500

type Mode = 'queue' | 'exchange_source' | 'exchange_destination'

export function BindingsTable({ bindings, mode }: { bindings: Binding[]; mode: Mode }) {
  const unbind = useApiMutation<Binding>({ mutationFn: deleteBinding, success: 'Binding removed', invalidate: [['bindings']] })
  if (bindings.length === 0) return <p className="muted">No bindings</p>
  const shown = bindings.slice(0, MAX_BINDINGS)
  return (
    <>
      {bindings.length > MAX_BINDINGS ? (
        <p className="hint">Only the first {MAX_BINDINGS} of {bindings.length} bindings are shown.</p>
      ) : null}
      <div className="table-wrap">
        <table className="list" data-testid="bindings-table">
          <thead>
            <tr>
              <th>{mode === 'exchange_source' ? 'To' : 'From'}</th>
              <th>Routing key</th>
              <th>Arguments</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {shown.map((binding) =>
              binding.source === '' ? (
                <tr key={`${binding.source}|${binding.properties_key}`}>
                  <td colSpan={4} className="muted">
                    (Default exchange binding)
                  </td>
                </tr>
              ) : (
                <tr key={`${binding.source}|${binding.destination_type}|${binding.destination}|${binding.properties_key}`} data-testid="binding-row">
                  <td>
                    {mode !== 'exchange_source' ? (
                      <ExchangeLink vhost={binding.vhost} name={binding.source} />
                    ) : binding.destination_type === 'exchange' ? (
                      <>
                        <span className="tag">exchange</span>
                        <ExchangeLink vhost={binding.vhost} name={binding.destination} />
                      </>
                    ) : (
                      <>
                        <span className="tag">queue</span>
                        <QueueLink vhost={binding.vhost} name={binding.destination} />
                      </>
                    )}
                  </td>
                  <td className="mono">{binding.routing_key}</td>
                  <td>{fmtTableFlat(binding.arguments)}</td>
                  <td>
                    <button
                      type="button"
                      className="btn btn-small btn-danger"
                      disabled={unbind.isPending}
                      onClick={() => {
                        if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) unbind.mutate(binding)
                      }}
                    >
                      Unbind
                    </button>
                  </td>
                </tr>
              ),
            )}
          </tbody>
        </table>
      </div>
    </>
  )
}

export function AddBindingForm({ vhost, name, mode }: { vhost: string; name: string; mode: 'queue' | 'exchange' }) {
  const notify = useNotify()
  const [other, setOther] = useState('')
  const [destinationType, setDestinationType] = useState<'q' | 'e'>('q')
  const [routingKey, setRoutingKey] = useState('')
  const [args, setArgs] = useState<ArgRow[]>([])
  const bind = useApiMutation<{ source: string; type: 'q' | 'e'; destination: string; routing_key: string; arguments: ReturnType<typeof rowsToTable> }>({
    mutationFn: (v) => addBinding(vhost, v.source, v.type, v.destination, { routing_key: v.routing_key, arguments: v.arguments }),
    success: 'Binding added',
    invalidate: [['bindings']],
    onSuccess: () => {
      setOther('')
      setRoutingKey('')
      setArgs([])
    },
  })

  const submit = (event: FormEvent) => {
    event.preventDefault()
    let table
    try {
      table = rowsToTable(args)
    } catch (err) {
      notify('error', errorMessage(err))
      return
    }
    if (mode === 'queue') bind.mutate({ source: other, type: 'q', destination: name, routing_key: routingKey, arguments: table })
    else bind.mutate({ source: name, type: destinationType, destination: other, routing_key: routingKey, arguments: table })
  }

  return (
    <form className="form" onSubmit={submit} data-testid="add-binding-form">
      <div className="full">
        <h3>{mode === 'queue' ? 'Add binding to this queue' : 'Add binding from this exchange'}</h3>
      </div>
      {mode === 'queue' ? (
        <label htmlFor="binding-source">From exchange</label>
      ) : (
        <select aria-label="Destination type" value={destinationType} onChange={(e) => setDestinationType(e.target.value as 'q' | 'e')} style={{ justifySelf: 'end' }}>
          <option value="q">To queue</option>
          <option value="e">To exchange</option>
        </select>
      )}
      <input id="binding-source" type="text" value={other} onChange={(e) => setOther(e.target.value)} required data-testid="binding-other" />
      <label htmlFor="binding-key">Routing key</label>
      <input id="binding-key" type="text" value={routingKey} onChange={(e) => setRoutingKey(e.target.value)} data-testid="binding-routing-key" />
      <span className="label">Arguments</span>
      <ArgumentsEditor rows={args} onChange={setArgs} testId="binding-arguments" />
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={bind.isPending} data-testid="binding-submit">
          Bind
        </button>
      </div>
    </form>
  )
}
