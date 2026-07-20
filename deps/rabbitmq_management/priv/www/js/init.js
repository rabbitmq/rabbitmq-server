// Page initialisation module.
//
// This module replaces the inline <script type="module"> that previously
// existed in index.html.  Moving it to an external file allows the
// Content-Security-Policy to drop the 'unsafe-inline' directive.
//
// Execution order: module scripts are deferred and run after all preceding
// module scripts in document order.  bootstrap.js (from the oauth2 plugin,
// loaded just before this) will have run first and may have called
// set_oauth_settings() to populate the OAuth configuration.

import { oauth_initialize_if_required } from './oidc-oauth/helper.js';

check_version();
window.oauth = oauth_initialize_if_required();
