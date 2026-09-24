import { Link, Outlet, useMatches, useRouterState } from '@tanstack/react-router'
import { useQueryClient } from '@tanstack/react-query'
import { logout } from '../api/resources/overview'
import { clearSession } from '../auth/session'
import type { UserTag } from '../auth/access'
import { useColorScheme, type ColorScheme } from './theme'
import { useAppData } from './context'
import { canSee, isGroup, NAVIGATION, testIdFor, visibleNavigation, withPlugins, type NavLink } from './navigation'
import { REFRESH_OPTIONS, RefreshProvider, useRefresh } from './refresh'
import { useSelectedVhost } from './vhost'
import { classicHref, CLASSIC_BASE } from './classic'
import styles from './AppShell.module.css'

declare module '@tanstack/react-router' {
  interface StaticDataRouteOption {
    requires?: { tag: UserTag; needsVhosts?: boolean }
  }
}

export function AppShell() {
  return (
    <RefreshProvider>
      <div className={styles.shell}>
        <Header />
        <Navigation />
        <Content />
      </div>
    </RefreshProvider>
  )
}

function Header() {
  const { overview, access } = useAppData()
  const queryClient = useQueryClient()
  const [scheme, setScheme] = useColorScheme()
  // Re-render on navigation so that the classic UI link follows the current screen.
  useRouterState({ select: (state) => state.location.href })

  const product =
    overview.product_name && overview.product_version
      ? `${overview.product_name} ${overview.product_version}`
      : `RabbitMQ ${overview.rabbitmq_version}`

  const signOut = async () => {
    await logout()
    clearSession()
    queryClient.clear()
  }

  return (
    <header className={styles.header}>
      <Link to="/" className={styles.brand}>
        <span className={styles.logo} aria-hidden="true" />
        <span>RabbitMQ</span>
        <span className={styles.preview}>next</span>
      </Link>
      <div className={styles.cluster}>
        <span className="muted">Cluster</span>{' '}
        {access.isAdministrator ? (
          <Link to="/cluster-name" data-testid="cluster-name">
            {overview.cluster_name}
          </Link>
        ) : (
          <span data-testid="cluster-name">{overview.cluster_name}</span>
        )}
        <span className={styles.versions}>
          <abbr title={`Available exchange types: ${overview.exchange_types.map((t) => t.name).join(', ')}`}>{product}</abbr>
          <abbr title={overview.erlang_full_version}>Erlang {overview.erlang_version}</abbr>
        </span>
      </div>
      <div className={styles.controls}>
        <RefreshControl />
        <VhostSelector />
        <select
          aria-label="Colour scheme"
          value={scheme}
          onChange={(event) => setScheme(event.target.value as ColorScheme)}
          data-testid="theme"
        >
          <option value="auto">Auto theme</option>
          <option value="light">Light</option>
          <option value="dark">Dark</option>
        </select>
        <a className="btn btn-small" href={classicHref()} data-testid="classic-ui-link" title="Open this screen in the classic UI">
          Classic UI
        </a>
        <span className={styles.user}>
          <span className="muted">User</span>{' '}
          {access.isAdministrator && access.user.is_internal_user ? (
            <Link to="/users/$name" params={{ name: access.user.name }} data-testid="current-user">
              {access.user.name}
            </Link>
          ) : (
            <span data-testid="current-user">{access.user.name}</span>
          )}
        </span>
        <button type="button" className="btn btn-small" onClick={signOut} data-testid="logout">
          Log out
        </button>
      </div>
    </header>
  )
}

function RefreshControl() {
  const { intervalPref, setInterval, paused, togglePaused, interval } = useRefresh()
  return (
    <span className={styles.refresh}>
      <select aria-label="Refresh interval" value={intervalPref} onChange={(event) => setInterval(event.target.value)} data-testid="refresh-interval">
        {REFRESH_OPTIONS.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
      <button
        type="button"
        className="btn btn-small"
        onClick={togglePaused}
        disabled={interval === false}
        aria-pressed={paused}
        title={paused ? 'Resume refreshing this screen' : 'Hold this screen still'}
        data-testid="refresh-pause"
      >
        {paused ? '▶ Resume' : '❚❚ Pause'}
      </button>
    </span>
  )
}

function VhostSelector() {
  const { vhosts, access } = useAppData()
  const [vhost, setVhost] = useSelectedVhost()
  if (!access.canAccessVhosts) return null
  return (
    <label className={styles.vhost}>
      <span className="muted">Virtual host</span>
      <select value={vhost} onChange={(event) => setVhost(event.target.value)} data-testid="vhost-selector">
        <option value="">All</option>
        {vhosts.map((v) => (
          <option key={v.name} value={v.name}>
            {v.name}
          </option>
        ))}
      </select>
    </label>
  )
}

function useNavigationEntries() {
  const { access, extensionScripts } = useAppData()
  return visibleNavigation(access, withPlugins(NAVIGATION, extensionScripts))
}

function isActive(pathname: string, to: string): boolean {
  return to === '/' ? pathname === '/' || pathname.startsWith('/nodes/') : pathname === to || pathname.startsWith(`${to}/`)
}

function Navigation() {
  const entries = useNavigationEntries()
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  return (
    <nav className={styles.nav} aria-label="Main">
      <ul>
        {entries.map((entry) => {
          const target = isGroup(entry) ? entry.children[0] : entry
          const active = isGroup(entry) ? entry.children.some((child) => isActive(pathname, child.to)) : isActive(pathname, entry.to)
          return (
            <li key={entry.label}>
              <NavAnchor link={target} label={entry.label} active={active} testId={testIdFor(entry.label)} />
            </li>
          )
        })}
      </ul>
    </nav>
  )
}

function NavAnchor({ link, label, active, testId }: { link: NavLink; label: string; active: boolean; testId: string }) {
  if (link.classic) {
    return (
      <a href={`${CLASSIC_BASE}#${link.to}`} className={styles.navLink} data-testid={testId} title="Opens in the classic UI">
        {label} <span className={styles.external}>↗</span>
      </a>
    )
  }
  return (
    <Link to={link.to} className={active ? `${styles.navLink} ${styles.active}` : styles.navLink} data-testid={testId}>
      {label}
    </Link>
  )
}

function Content() {
  const { access } = useAppData()
  const entries = useNavigationEntries()
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const requires = useMatches({ select: (matches) => matches.at(-1)?.staticData.requires })
  const admin = entries.find((entry) => isGroup(entry) && entry.label === 'Admin')
  const adminLinks = admin && isGroup(admin) ? admin.children : []
  const inAdmin = adminLinks.some((link) => !link.classic && isActive(pathname, link.to))
  const permitted = !requires || canSee(access, requires)

  return (
    <div className={inAdmin ? `${styles.content} ${styles.withSidebar}` : styles.content}>
      {inAdmin ? (
        <aside className={styles.sidebar} aria-label="Admin">
          <ul>
            {adminLinks.map((link) => (
              <li key={link.label}>
                <NavAnchor link={link} label={link.label} active={isActive(pathname, link.to)} testId={testIdFor(`admin ${link.label}`)} />
              </li>
            ))}
          </ul>
        </aside>
      ) : null}
      <main className={styles.main}>
        {permitted ? (
          <Outlet />
        ) : (
          <div className="callout callout-warn" data-testid="not-permitted">
            This screen is not available to user <strong>{access.user.name}</strong>.{' '}
            {requires && !access.hasTag(requires.tag) ? (
              <>
                It requires the <code>{requires.tag}</code> tag.
              </>
            ) : (
              'It requires permissions on at least one virtual host.'
            )}
          </div>
        )}
      </main>
    </div>
  )
}
