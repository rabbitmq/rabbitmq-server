import { createHashHistory, createRootRoute, createRoute, createRouter, Link } from '@tanstack/react-router'
import { AppShell } from './AppShell'
import { validateListSearch } from '../components/listState'
import { OverviewPage } from '../features/overview/OverviewPage'
import { NodePage } from '../features/nodes/NodePage'
import { ConnectionsPage } from '../features/connections/ConnectionsPage'
import { ConnectionPage } from '../features/connections/ConnectionPage'
import { ChannelsPage } from '../features/channels/ChannelsPage'
import { ChannelPage } from '../features/channels/ChannelPage'
import { ExchangesPage } from '../features/exchanges/ExchangesPage'
import { ExchangePage } from '../features/exchanges/ExchangePage'
import { QueuesPage } from '../features/queues/QueuesPage'
import { QueuePage } from '../features/queues/QueuePage'
import { UsersPage } from '../features/users/UsersPage'
import { UserPage } from '../features/users/UserPage'
import { VhostsPage } from '../features/vhosts/VhostsPage'
import { VhostPage } from '../features/vhosts/VhostPage'
import { PoliciesPage } from '../features/policies/PoliciesPage'
import { PolicyPage } from '../features/policies/PolicyPage'
import { LimitsPage } from '../features/limits/LimitsPage'
import { FeatureFlagsPage } from '../features/feature-flags/FeatureFlagsPage'
import { DeprecatedFeaturesPage } from '../features/deprecated-features/DeprecatedFeaturesPage'
import { ClusterNamePage } from '../features/cluster/ClusterNamePage'

function NotFound() {
  return (
    <div className="callout callout-warn" data-testid="not-found">
      There is no such screen. <Link to="/">Go to the overview</Link>.
    </div>
  )
}

const rootRoute = createRootRoute({ component: AppShell, notFoundComponent: NotFound })

const management = { requires: { tag: 'management' as const } }
const withVhosts = { requires: { tag: 'management' as const, needsVhosts: true } }
const administrator = { requires: { tag: 'administrator' as const } }

// The paths mirror the classic UI's Sammy routes in dispatcher.js, so that links
// and bookmarks carry over between the two UIs.
const routes = [
  createRoute({ getParentRoute: () => rootRoute, path: '/', component: OverviewPage, staticData: management }),
  createRoute({ getParentRoute: () => rootRoute, path: '/nodes/$name', component: NodePage, staticData: management }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: '/connections',
    component: ConnectionsPage,
    staticData: withVhosts,
    validateSearch: validateListSearch,
  }),
  createRoute({ getParentRoute: () => rootRoute, path: '/connections/$name', component: ConnectionPage, staticData: withVhosts }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: '/channels',
    component: ChannelsPage,
    staticData: withVhosts,
    validateSearch: validateListSearch,
  }),
  createRoute({ getParentRoute: () => rootRoute, path: '/channels/$name', component: ChannelPage, staticData: withVhosts }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: '/exchanges',
    component: ExchangesPage,
    staticData: withVhosts,
    validateSearch: validateListSearch,
  }),
  createRoute({ getParentRoute: () => rootRoute, path: '/exchanges/$vhost/$name', component: ExchangePage, staticData: withVhosts }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: '/queues',
    component: QueuesPage,
    staticData: withVhosts,
    validateSearch: validateListSearch,
  }),
  createRoute({ getParentRoute: () => rootRoute, path: '/queues/$vhost/$name', component: QueuePage, staticData: withVhosts }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: '/users',
    component: UsersPage,
    staticData: administrator,
    validateSearch: validateListSearch,
  }),
  createRoute({ getParentRoute: () => rootRoute, path: '/users/$name', component: UserPage, staticData: administrator }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: '/vhosts',
    component: VhostsPage,
    staticData: administrator,
    validateSearch: validateListSearch,
  }),
  createRoute({ getParentRoute: () => rootRoute, path: '/vhosts/$name', component: VhostPage, staticData: administrator }),
  createRoute({ getParentRoute: () => rootRoute, path: '/policies', component: PoliciesPage, staticData: withVhosts }),
  createRoute({ getParentRoute: () => rootRoute, path: '/policies/$vhost/$name', component: PolicyPage, staticData: withVhosts }),
  createRoute({ getParentRoute: () => rootRoute, path: '/limits', component: LimitsPage, staticData: withVhosts }),
  createRoute({ getParentRoute: () => rootRoute, path: '/feature-flags', component: FeatureFlagsPage, staticData: administrator }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: '/deprecated-features',
    component: DeprecatedFeaturesPage,
    staticData: administrator,
  }),
  createRoute({ getParentRoute: () => rootRoute, path: '/cluster-name', component: ClusterNamePage, staticData: administrator }),
] as const

const routeTree = rootRoute.addChildren(routes)

export function createAppRouter() {
  return createRouter({ routeTree, history: createHashHistory(), scrollRestoration: true })
}

declare module '@tanstack/react-router' {
  interface Register {
    router: ReturnType<typeof createAppRouter>
  }
}
