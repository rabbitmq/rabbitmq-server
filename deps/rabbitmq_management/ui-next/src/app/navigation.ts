import type { Access, UserTag } from '../auth/access'

export interface NavLink {
  label: string
  to: string
  tag: UserTag
  needsVhosts?: boolean
  /** A plugin page that exists only in the classic UI. */
  classic?: boolean
}

export interface NavGroup {
  label: string
  tag: UserTag
  needsVhosts?: boolean
  children: NavLink[]
}

export type NavEntry = NavLink | NavGroup

export const isGroup = (entry: NavEntry): entry is NavGroup => 'children' in entry

/** A typed copy of `NAVIGATION` in the classic UI's global.js. */
export const NAVIGATION: NavEntry[] = [
  { label: 'Overview', to: '/', tag: 'management' },
  { label: 'Connections', to: '/connections', tag: 'management', needsVhosts: true },
  { label: 'Channels', to: '/channels', tag: 'management', needsVhosts: true },
  { label: 'Exchanges', to: '/exchanges', tag: 'management', needsVhosts: true },
  { label: 'Queues and Streams', to: '/queues', tag: 'management', needsVhosts: true },
  {
    label: 'Admin',
    tag: 'management',
    needsVhosts: true,
    children: [
      { label: 'Users', to: '/users', tag: 'administrator' },
      { label: 'Virtual Hosts', to: '/vhosts', tag: 'administrator' },
      { label: 'Feature Flags', to: '/feature-flags', tag: 'administrator' },
      { label: 'Deprecated Features', to: '/deprecated-features', tag: 'administrator' },
      { label: 'Policies', to: '/policies', tag: 'management' },
      { label: 'Limits', to: '/limits', tag: 'management' },
      { label: 'Cluster', to: '/cluster-name', tag: 'administrator' },
    ],
  },
]

interface PluginPages {
  script: string
  topLevel?: NavLink[]
  admin?: NavLink[]
}

/**
 * Pages that plugins add to the classic UI through `web_ui/0`. They are listed
 * here and link into the classic UI until plugins have a contract with this one.
 */
const PLUGIN_PAGES: PluginPages[] = [
  {
    script: 'stream.js',
    topLevel: [
      { label: 'Stream Connections', to: '/stream/connections', tag: 'monitoring', needsVhosts: true, classic: true },
      { label: 'Super Streams', to: '/stream/super-streams', tag: 'management', needsVhosts: true, classic: true },
    ],
  },
  {
    script: 'shovel.js',
    admin: [
      { label: 'Shovel Status', to: '/shovels', tag: 'monitoring', classic: true },
      { label: 'Shovel Management', to: '/dynamic-shovels', tag: 'policymaker', classic: true },
    ],
  },
  {
    script: 'federation.js',
    admin: [
      { label: 'Federation Status', to: '/federation', tag: 'monitoring', classic: true },
      { label: 'Federation Upstreams', to: '/federation-upstreams', tag: 'policymaker', classic: true },
    ],
  },
  {
    script: 'top.js',
    admin: [
      { label: 'Top Processes', to: '/top', tag: 'administrator', classic: true },
      { label: 'Top ETS Tables', to: '/top/ets', tag: 'administrator', classic: true },
    ],
  },
  { script: 'tracing.js', admin: [{ label: 'Tracing', to: '/traces', tag: 'administrator', classic: true }] },
]

export function withPlugins(navigation: NavEntry[], scripts: Set<string>): NavEntry[] {
  const plugins = PLUGIN_PAGES.filter((plugin) => scripts.has(plugin.script))
  const topLevel = plugins.flatMap((plugin) => plugin.topLevel ?? [])
  const admin = plugins.flatMap((plugin) => plugin.admin ?? [])
  return navigation.flatMap((entry) => {
    if (isGroup(entry) && entry.label === 'Admin') return [...topLevel, { ...entry, children: [...entry.children, ...admin] }]
    return [entry]
  })
}

export function canSee(access: Access, entry: { tag: UserTag; needsVhosts?: boolean }): boolean {
  return access.hasTag(entry.tag) && (!entry.needsVhosts || access.canAccessVhosts)
}

/** The navigation this user may use: entries they cannot use are absent, not disabled. */
export function visibleNavigation(access: Access, navigation: NavEntry[]): NavEntry[] {
  return navigation.flatMap((entry): NavEntry[] => {
    if (!canSee(access, entry)) return []
    if (!isGroup(entry)) return [entry]
    const children = entry.children.filter((child) => canSee(access, child))
    return children.length > 0 ? [{ ...entry, children }] : []
  })
}

export function testIdFor(label: string): string {
  return `nav-${label.toLowerCase().replace(/\s+/g, '-')}`
}
