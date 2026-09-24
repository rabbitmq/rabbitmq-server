import { useAppData } from './context'
import { usePref } from '../prefs/storage'

/**
 * The vhost chosen in the header, or '' for all. It is stored in the classic
 * UI's `vhost` pref, and a vhost that no longer exists reads as "all".
 */
export function useSelectedVhost(): [string, (vhost: string) => void] {
  const { vhosts } = useAppData()
  const [pref, setPref] = usePref('vhost', '')
  const valid = pref === '' || vhosts.some((vhost) => vhost.name === pref)
  return [valid ? pref : '', setPref]
}
