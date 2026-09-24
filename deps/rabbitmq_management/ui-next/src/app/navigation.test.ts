import { describe, expect, it } from 'vitest'
import { buildAccess, expandUserTags } from '../auth/access'
import { isGroup, NAVIGATION, visibleNavigation, withPlugins, type NavEntry } from './navigation'

const user = (tags: string[]) => ({ name: 'u', tags, is_internal_user: true })
const vhost = { name: '/' }

function labels(entries: NavEntry[]): string[] {
  return entries.flatMap((e) => (isGroup(e) ? [e.label, ...e.children.map((c) => `${e.label} > ${c.label}`)] : [e.label]))
}

describe('expandUserTags', () => {
  it('implies tags as the classic UI does', () => {
    expect([...expandUserTags(['administrator'])].sort()).toEqual(['administrator', 'management', 'monitoring', 'policymaker'])
    expect([...expandUserTags(['monitoring'])].sort()).toEqual(['management', 'monitoring'])
    expect([...expandUserTags(['policymaker'])].sort()).toEqual(['management', 'policymaker'])
    expect([...expandUserTags(['impersonator'])]).toEqual(['impersonator'])
  })
})

describe('visibleNavigation', () => {
  it('shows everything to an administrator', () => {
    const nav = visibleNavigation(buildAccess(user(['administrator']), [vhost]), NAVIGATION)
    expect(labels(nav)).toContain('Admin > Users')
    expect(labels(nav)).toContain('Admin > Cluster')
  })

  it('leaves administrator entries out for a management user rather than disabling them', () => {
    const nav = labels(visibleNavigation(buildAccess(user(['management']), [vhost]), NAVIGATION))
    expect(nav).toEqual([
      'Overview',
      'Connections',
      'Channels',
      'Exchanges',
      'Queues and Streams',
      'Admin',
      'Admin > Policies',
      'Admin > Limits',
    ])
  })

  it('hides vhost-scoped entries from a user without access to any vhost', () => {
    expect(labels(visibleNavigation(buildAccess(user(['management']), []), NAVIGATION))).toEqual(['Overview'])
  })

  it('adds plugin pages only for the plugins that are enabled', () => {
    const nav = withPlugins(NAVIGATION, new Set(['shovel.js', 'stream.js']))
    const all = labels(visibleNavigation(buildAccess(user(['administrator']), [vhost]), nav))
    expect(all).toContain('Admin > Shovel Status')
    expect(all).toContain('Stream Connections')
    expect(all).not.toContain('Admin > Tracing')
    const monitoring = labels(visibleNavigation(buildAccess(user(['monitoring']), [vhost]), nav))
    expect(monitoring).toContain('Admin > Shovel Status')
    expect(monitoring).not.toContain('Admin > Shovel Management')
  })
})
