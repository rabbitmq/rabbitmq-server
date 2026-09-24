import { Link } from '@tanstack/react-router'
import { exchangeName, shortChan, shortConn } from '../format/names'

export function QueueLink({ vhost, name }: { vhost: string; name: string }) {
  return (
    <Link to="/queues/$vhost/$name" params={{ vhost, name }} data-testid="queue-link">
      {name}
    </Link>
  )
}

export function ExchangeLink({ vhost, name }: { vhost: string; name: string }) {
  return (
    <Link to="/exchanges/$vhost/$name" params={{ vhost, name: name === '' ? 'amq.default' : name }} data-testid="exchange-link">
      {exchangeName(name)}
    </Link>
  )
}

export function ConnectionLink({ name, short = true }: { name: string; short?: boolean }) {
  return (
    <Link to="/connections/$name" params={{ name }} title={name}>
      {short ? shortConn(name) : name}
    </Link>
  )
}

export function ChannelLink({ name, short = true }: { name: string; short?: boolean }) {
  return (
    <Link to="/channels/$name" params={{ name }} title={name}>
      {short ? shortChan(name) : name}
    </Link>
  )
}

export function NodeLink({ name }: { name: string }) {
  return (
    <Link to="/nodes/$name" params={{ name }}>
      {name}
    </Link>
  )
}

export function VhostLink({ name }: { name: string }) {
  return (
    <Link to="/vhosts/$name" params={{ name }}>
      {name}
    </Link>
  )
}

export function UserLink({ name }: { name: string }) {
  return (
    <Link to="/users/$name" params={{ name }}>
      {name}
    </Link>
  )
}

export function PolicyLink({ vhost, name }: { vhost: string; name: string }) {
  return (
    <Link to="/policies/$vhost/$name" params={{ vhost, name }}>
      {name}
    </Link>
  )
}
