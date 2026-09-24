import type { Application } from '../api/types/nodes'
import type { AmqpTable } from '../api/types/common'

/** The client side of a connection name such as `127.0.0.1:5000 -> 127.0.0.1:5672`. */
export function shortConn(name: string): string {
  const match = /^(.*)->/.exec(name)
  return match ? match[1].trim() : name
}

/** A channel name without the server address, keeping the channel number. */
export function shortChan(name: string): string {
  const match = /^(.*)->.*( \(.*\))/.exec(name)
  return match ? `${match[1].trim()}${match[2]}` : name
}

export function exchangeName(name: string): string {
  return name === '' ? '(AMQP default)' : name
}

/** Masks the password in a URI, as `fmt_uri_with_credentials` does. */
export function maskUriCredentials(uri: string): string {
  return uri.replace(/^([a-zA-Z0-9+\-.]+):\/\/(.*):(.*)@/, '$1://$2:[redacted]@')
}

export function rabbitVersion(applications: Application[] | undefined): string {
  return applications?.find((app) => app.name === 'rabbit')?.version ?? 'unknown'
}

export interface ClientName {
  name: string
  version?: string
  clientId?: string
}

export function clientName(properties: AmqpTable | undefined): ClientName | undefined {
  if (!properties) return undefined
  const text = (key: string) => (typeof properties[key] === 'string' ? (properties[key] as string) : undefined)
  const parts = [text('product'), text('platform')].filter((p): p is string => p !== undefined)
  if (parts.length === 0 && text('version') === undefined && text('client_id') === undefined) return undefined
  return { name: parts.join(' / '), version: text('version'), clientId: text('client_id') }
}

/** A link from broker data, such as a feature flag's `doc_url`, is only followed if it is http or https. */
export function safeHttpUrl(url: string | undefined): string | undefined {
  if (!url) return undefined
  try {
    const parsed = new URL(url)
    return parsed.protocol === 'http:' || parsed.protocol === 'https:' ? parsed.href : undefined
  } catch {
    return undefined
  }
}
