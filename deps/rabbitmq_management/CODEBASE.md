# RabbitMQ Management Plugin Codebase Structure

## Overview

`rabbitmq_management` is a RabbitMQ plugin that provides a management UI and HTTP API for monitoring and administering RabbitMQ broker instances. It serves as the interface between users/applications and the RabbitMQ core server, exposing administrative operations and operational metrics.

## Directory Structure

```
rabbitmq_management/
├── src/                          # Erlang source code (115 modules)
│   ├── rabbit_mgmt_app.erl       # Application lifecycle and listener startup
│   ├── rabbit_mgmt_dispatcher.erl # HTTP routing configuration
│   ├── rabbit_mgmt_db.erl        # Management database and metrics aggregation
│   ├── rabbit_mgmt_stats.erl     # Metrics collection and computation
│   ├── rabbit_mgmt_util.erl      # Shared utilities and formatters
│   ├── rabbit_mgmt_wm_*.erl      # Web modules (~90 modules) handling specific API endpoints
│   ├── rabbit_mgmt_login.erl     # Authentication and session management
│   ├── rabbit_mgmt_oauth_*.erl   # OAuth 2.0 support
│   └── rabbit_mgmt_*.erl         # Core infrastructure (db cache, CORS, CSP, etc.)
├── test/                         # Common Test test suites
├── include/                      # Erlang header files
│   └── rabbit_mgmt.hrl           # Constants and macros
├── priv/                         # Runtime resources
│   ├── www/                      # Management UI and static assets
│   │   ├── index.html            # Main UI HTML file
│   │   ├── js/                   # JavaScript UI code
│   │   ├── css/                  # Stylesheets
│   │   ├── img/                  # Images and icons
│   │   ├── api/                  # API documentation
│   │   └── cli/                  # CLI documentation
│   └── schema/                   # Configuration schemas
├── ebin/                         # Compiled bytecode
├── sbin/                         # Executables
└── plugins/                      # Plugin artifacts
```

## Key Components

### Core Infrastructure

**`rabbit_mgmt_app.erl`**
- Application lifecycle management (start/stop)
- HTTP listener configuration and startup (TCP and TLS)
- Handles legacy and new configuration formats
- Registers HTTP contexts with Cowboy web server

**`rabbit_mgmt_dispatcher.erl`**
- Builds Cowboy HTTP routing rules
- Routes requests to appropriate web modules based on URL paths
- Handles static file serving (CSS, JavaScript, images)
- Manages path prefix configuration for deployments behind reverse proxies
- Registers OAuth bootstrap and token proxy routes

**`rabbit_mgmt_db.erl`**
- Aggregates node-local data across the cluster
- Responds to queries from web modules
- Coordinates with `rabbit_mgmt_stats` for metrics
- Implements caching via `rabbit_mgmt_db_cache`
- Provides methods to augment broker objects (exchanges, queues, nodes, vhosts) with operational metrics

### Metrics and Monitoring

**`rabbit_mgmt_stats.erl`**
- Collects and computes operational metrics
- Handles time-series data with configurable ranges
- Aggregates per-connection, per-channel, per-queue metrics
- Manages metric sampling and rate calculations

**`rabbit_mgmt_db_cache.erl`**
- Caches aggregated data to reduce computational overhead
- Manages cache invalidation and TTL

### Authentication and Security

**`rabbit_mgmt_login.erl`**
- Handles user login and session creation
- Supports multiple authentication mechanisms
- Manages login-attempt tracking (for abuse prevention)

**`rabbit_mgmt_oauth_*.erl`** (multiple modules)
- OAuth 2.0/OIDC support
- Bootstrap configuration delivery
- Token proxy for credential management
- Integration with external identity providers

**`rabbit_mgmt_cors.erl`**
- CORS (Cross-Origin Resource Sharing) handling
- Allows cross-origin HTTP requests with proper headers

**`rabbit_mgmt_csp.erl`**
- Content Security Policy implementation
- Restricts resource loading for security

### Utilities

**`rabbit_mgmt_util.erl`**
- Shared formatting and conversion functions
- Response envelope building
- Path prefix management
- Common field extraction helpers

### Web Modules (API Endpoints)

The `rabbit_mgmt_wm_*.erl` modules (approximately 90 of them) implement specific HTTP API endpoints using the Cowboy/WebMachine pattern:

**Resource Management**
- `rabbit_mgmt_wm_exchanges.erl` - GET/POST exchanges
- `rabbit_mgmt_wm_queues.erl` - GET/POST queues
- `rabbit_mgmt_wm_bindings.erl` - GET/POST bindings
- `rabbit_mgmt_wm_policies.erl` - GET/POST policies

**Access Control**
- `rabbit_mgmt_wm_users.erl` - User management
- `rabbit_mgmt_wm_permissions*.erl` - Permission/access control
- `rabbit_mgmt_wm_vhosts.erl` - Virtual host management

**Monitoring and Operations**
- `rabbit_mgmt_wm_overview.erl` - Cluster overview
- `rabbit_mgmt_wm_nodes.erl` - Node status
- `rabbit_mgmt_wm_connections.erl` - Active connections
- `rabbit_mgmt_wm_channels.erl` - Active channels
- `rabbit_mgmt_wm_consumers.erl` - Consumer information
- `rabbit_mgmt_wm_healthchecks.erl` - Health check endpoints

**Queue Operations**
- `rabbit_mgmt_wm_queue_get.erl` - Get individual messages from queue (for debugging)
- `rabbit_mgmt_wm_queue_purge.erl` - Purge queue
- `rabbit_mgmt_wm_queue_actions.erl` - General queue operations

**Testing and Diagnostics**
- `rabbit_mgmt_wm_aliveness_test.erl` - Aliveness check
- `rabbit_mgmt_wm_auth_attempts.erl` - Authentication attempt logs

**Advanced Features**
- `rabbit_mgmt_wm_quorum_queue_*.erl` - Quorum queue operations (add/remove members)
- `rabbit_mgmt_wm_rebalance_queues.erl` - Queue rebalancing
- `rabbit_mgmt_wm_definitions.erl` - Backup/restore broker configuration

## Request Flow

1. **HTTP Request** → Cowboy receives request on configured port (15672 or 15671)
2. **Routing** → `rabbit_mgmt_dispatcher` matches URL pattern to web module
3. **Authentication** → Web module verifies user credentials (via LDAP, OAuth, internal DB, etc.)
4. **Authorization** → RabbitMQ checks permissions for requested operation
5. **Data Aggregation** → Web module calls `rabbit_mgmt_db` for operational data
6. **Metrics** → `rabbit_mgmt_db` collects stats from all nodes via `rabbit_mgmt_stats`
7. **Caching** → Results cached to avoid repeated aggregation
8. **Response** → Web module formats results as JSON and returns to client

## UI and Frontend

The management UI is a single-page application (SPA) served from `priv/www/`:

- **`index.html`** - Bootstrap HTML file
- **`js/`** - JavaScript application code
- **`css/`** - Stylesheets
- **`api/`** - OpenAPI/Swagger API documentation
- **`cli/`** - CLI tool documentation

## Configuration

Configuration is handled through RabbitMQ's standard `rabbitmq.conf`:

- HTTP listener ports and SSL settings
- Load definition files on startup
- Custom path prefix for reverse proxy deployments
- OAuth/OIDC provider settings
- Feature flags and experimental features

## Testing

Test suites cover:
- HTTP API functionality (`rabbit_mgmt_http_SUITE.erl`)
- Statistics computation (`stats_SUITE.erl`)
- Database operations (`rabbit_mgmt_test_db_SUITE.erl`)
- Health checks (`rabbit_mgmt_http_health_checks_SUITE.erl`)
- OAuth flows (`rabbit_mgmt_oauth_token_proxy_SUITE.erl`)
- Integration tests with clustering (`clustering_SUITE.erl`)

## Key Design Patterns

1. **WebMachine Pattern** - Web modules follow REST conventions with resource-oriented endpoints
2. **Cluster Aggregation** - Data is collected from all nodes and aggregated by the management database
3. **Lazy Evaluation** - Metrics computed on-demand rather than continuously stored
4. **Caching Strategy** - In-memory caching reduces recalculation overhead
5. **Extension System** - Plugins can register additional routes via `rabbit_mgmt_extension` behavior
6. **Authentication Abstraction** - Pluggable authentication backends (LDAP, OAuth, HTTP, internal)
