import type { Vhost } from '../api/types/admin'
import type { Whoami } from '../api/types/admin'

export type UserTag = 'administrator' | 'monitoring' | 'policymaker' | 'management'

/** Implied tags, as in `expand_user_tags` in the classic UI's global.js. */
export function expandUserTags(tags: readonly string[]): Set<string> {
  const expanded = new Set<string>()
  for (const tag of tags) {
    expanded.add(tag)
    switch (tag) {
      case 'administrator':
        expanded.add('monitoring').add('policymaker').add('management')
        break
      case 'monitoring':
      case 'policymaker':
        expanded.add('management')
        break
    }
  }
  return expanded
}

export interface Access {
  user: Whoami
  tags: Set<string>
  isAdministrator: boolean
  isMonitoring: boolean
  isPolicymaker: boolean
  canAccessVhosts: boolean
  hasTag(tag: UserTag): boolean
}

export function buildAccess(user: Whoami, vhosts: readonly Vhost[]): Access {
  const tags = expandUserTags(user.tags)
  return {
    user,
    tags,
    isAdministrator: tags.has('administrator'),
    isMonitoring: tags.has('monitoring'),
    isPolicymaker: tags.has('policymaker'),
    canAccessVhosts: vhosts.length > 0,
    hasTag: (tag) => tags.has(tag),
  }
}
