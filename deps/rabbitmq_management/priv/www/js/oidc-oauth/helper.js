import {oidc} from './oidc-client-ts.3.0.1.min.js';
import { replace_content, format, setup_visibility, toggle_visibility } from '../render.js';
import {
  authOptions,
  auth_options_for_mechanism,
  auth_section_is_expanded
} from '../auth-options.js';
import { registerAuthProvider } from '../auth-providers.js';

var mgr;
var _management_logger;


function rabbit_base_uri() {
  return window.location.protocol + "//" + window.location.hostname + rabbit_port() + rabbit_path_prefix()
}
function rabbit_path_prefix() {
  return window.location.pathname.replace(/(\/js\/oidc-oauth\/.*$|\/+$)/, "");
}
function rabbit_port() {
  return window.location.port ? ":" +  window.location.port : "";
}
function readiness_url(resource_server) {
    if (!resource_server.oauth_metadata_url) {
      return resource_server.oauth_provider_url + "/.well-known/openid-configuration"
    }else {
      return resource_server.oauth_metadata_url
    }
}

function auth_settings_apply_defaults(authSettings) {
  if (authSettings.oauth_provider_url) {
    if (!authSettings.oauth_response_type) {
      authSettings.oauth_response_type = "code"; // although the default value in oidc client
    }
    if (!authSettings.oauth_scopes) {
      authSettings.oauth_scopes = "openid profile";
    }
    if (!authSettings.oauth_initiated_logon_type) {
      authSettings.oauth_initiated_logon_type = "sp_initiated"
    }
  }
  authSettings.resource_servers = []

  if (authSettings.oauth_resource_servers) {

    for (const [resource_server_id, resource_server] of Object.entries(authSettings.oauth_resource_servers)) {
        if (!resource_server.oauth_provider_url) {
          resource_server.oauth_provider_url = authSettings.oauth_provider_url
        }
        if (!resource_server.oauth_provider_url) {
          break
        }
        if (!resource_server.oauth_response_type) {
          resource_server.oauth_response_type = authSettings.oauth_response_type
        }
        if (!resource_server.oauth_scopes) {
          resource_server.oauth_scopes = authSettings.oauth_scopes
        }
        if (!resource_server.oauth_client_id) {
          resource_server.oauth_client_id = authSettings.oauth_client_id
        }
        if (!resource_server.oauth_initiated_logon_type) {
          if (authSettings.oauth_initiated_logon_type) {
            resource_server.oauth_initiated_logon_type = authSettings.oauth_initiated_logon_type
          }else {
            resource_server.oauth_initiated_logon_type = "sp_initiated"
          }
        }
        if (resource_server.oauth_initiated_logon_type == "idp_initiated") {
          resource_server.sp_initiated = false
        } else {
          resource_server.sp_initiated = true
        }
        if (!resource_server.oauth_metadata_url) {
          resource_server.oauth_metadata_url = authSettings.metadata_url
        }
        if (!resource_server.oauth_authorization_endpoint_params) {
          resource_server.oauth_authorization_endpoint_params =
            authSettings.oauth_authorization_endpoint_params
        }
        if (!resource_server.oauth_token_endpoint_params) {
          resource_server.oauth_token_endpoint_params =
            authSettings.oauth_token_endpoint_params
        }
        resource_server.id = resource_server_id
        authSettings.resource_servers.push(resource_server)
    }
  }

  return authSettings;
}

var oauth_settings = { oauth_enabled : false}

export function set_oauth_settings(settings) {
  oauth_settings = settings
}
function get_oauth_settings() {
  return oauth_settings
}

export function oauth_initialize_if_required(state = "index") {
  let oauth = oauth_initialize(get_oauth_settings())
  if (!oauth.enabled) return oauth;

  switch (state) {
    case 'login-callback':
      oauth_completeLogin(); break;
    case 'logout-callback':
      oauth_completeLogout(); break;
    default:
      if (has_auth_credentials(BASIC_AUTH_SCHEME)) {
        break;
      } else {
        oauth = oauth_initiate(oauth);
      }
  }
  return oauth;
}

export function oauth_initiate(oauth) {
  if (oauth.enabled) {
    if (!oauth.sp_initiated) {
        oauth.logged_in = has_auth_credentials();
        // An idp-initiated session never goes through oauth_initiateLogin(),
        // the only other place that declares the active provider, so
        // active_auth_provider()'s has_auth_resource() fallback would
        // otherwise misidentify this session as basic auth.
        if (oauth.logged_in) set_active_auth_provider_by_name('oauth2');
    } else {
      oauth_is_logged_in().then( status => {
        if (status.loggedIn && !has_auth_credentials()) {
          oauth.logged_in = false;
          oauth_initiateLogout();
        } else {
          if (!status.loggedIn) {
            clear_auth();
          } else {
            oauth.logged_in = true;
            oauth.expiryDate = new Date(status.user.expires_at * 1000);  // it is epoch in seconds
            let current = new Date();
            _management_logger.debug('token expires in ', (oauth.expiryDate-current)/1000,
              'secs at : ', oauth.expiryDate );
            oauth.user_name = status.user.profile['user_name'];
            if (!oauth.user_name || oauth.user_name == '') {
              oauth.user_name = status.user.profile['sub'];
            }
            oauth.scopes = status.user.scope;
          }
        }
      });
    }
  }
  return oauth;
}
export function oidc_settings_from(resource_server) {
  let oidcSettings = {
    userStore: new oidc.WebStorageStateStore({ store: window.localStorage }),
    authority: resource_server.oauth_provider_url,
    metadataUrl: resource_server.oauth_metadata_url,
    client_id: resource_server.oauth_client_id,
    response_type: resource_server.oauth_response_type,
    scope: resource_server.oauth_scopes,
    redirect_uri: rabbit_base_uri() + "/js/oidc-oauth/login-callback.html",
    post_logout_redirect_uri: rabbit_base_uri() + "/",
    automaticSilentRenew: true,
    revokeAccessTokenOnSignout: true
  }
  if (resource_server.end_session_endpoint != "") {
    oidcSettings.metadataSeed = {
      end_session_endpoint: resource_server.end_session_endpoint
    }
  }
  if (resource_server.use_token_endpoint_proxy) {
    // The client secret stays server-side. Discover through the proxy so that
    // token requests carrying the secret are sent to RabbitMQ, not the provider.
    oidcSettings.metadataUrl = rabbit_base_uri() + "/js/oidc-oauth/token-endpoint/"
      + encodeURIComponent(resource_server.id) + "/openid-configuration"
  }
  if (resource_server.oauth_authorization_endpoint_params) {
    oidcSettings.extraQueryParams = resource_server.oauth_authorization_endpoint_params
  }
  if (resource_server.oauth_token_endpoint_params) {
    oidcSettings.extraTokenParams = resource_server.oauth_token_endpoint_params
  }
  return oidcSettings
}

function oauth_initialize_user_manager(resource_server) {
    oidc.Log.setLevel(oidc.Log.DEBUG);
    oidc.Log.setLogger(console);

    mgr = new oidc.UserManager(oidc_settings_from(resource_server))

    _management_logger = new oidc.Logger("Management");

    mgr.events.addAccessTokenExpiring(function() {
      _management_logger.info("token expiring...");
    });
    mgr.events.addAccessTokenExpired(function() {
      _management_logger.info("token expired!!");
    });
    mgr.events.addSilentRenewError(function(err) {
      _management_logger.error("token expiring failed due to ", err);
    });
    mgr.events.addUserLoaded(function(user) {
      set_token_auth(user.access_token)
    });

}

export function oauth_initialize(authSettings) {
    authSettings = auth_settings_apply_defaults(authSettings);
    let oauth = {
      "logged_in": false,
      "enabled" : authSettings.oauth_enabled,
      "resource_servers" : authSettings.resource_servers,
      "oauth_disable_basic_auth" : authSettings.oauth_disable_basic_auth,
    }
    if (!oauth.enabled) return oauth;
    
    if (authSettings.resource_servers.length > 1 || !authSettings.oauth_disable_basic_auth) {
      if (authSettings.strict_auth_mechanism) {
        oauth["strict_auth_mechanism"] = authSettings.strict_auth_mechanism;
      }else if (authSettings.preferred_auth_mechanism) {
        oauth["preferred_auth_mechanism"] = authSettings.preferred_auth_mechanism;
      }
    }
    let resource_server = null;

    if (oauth.resource_servers.length == 1) {
      resource_server = oauth.resource_servers[0]
    } else if (has_auth_resource()) {
      resource_server = lookup_resource_server(get_auth_resource(), oauth.resource_servers)
    }

    if (resource_server) {
      oauth.sp_initiated = resource_server.sp_initiated
      oauth.authority = resource_server.oauth_provider_url
      if (!resource_server.sp_initiated) return oauth;
      else oauth_initialize_user_manager(resource_server)
    }

    return oauth;
}

function oauth_is_logged_in() {
    return mgr.getUser().then(user => {
        if (!user) {
            return { "loggedIn": false };
        }
        return { "user": user, "loggedIn": !user.expired };
    });
}
function lookup_resource_server(resource_server_id, resource_servers) {
  let i = 0;

  while (i < resource_servers.length && resource_servers[i].id != resource_server_id) {
    i++;
  }
  if (i < resource_servers.length) return resource_servers[i]
  else return null
}

export function oauth_initiateLogin(resource_server_id) {
  let resource_server = lookup_resource_server(resource_server_id, oauth.resource_servers)
  if (!resource_server) return;
  set_auth_resource(resource_server_id)

  store_pref("oauth-return-to", window.location.hash);

  oauth.sp_initiated = resource_server.sp_initiated
  oauth.authority = resource_server.oauth_provider_url

  if (resource_server.sp_initiated) {
    if (!mgr) oauth_initialize_user_manager(resource_server)

    mgr.signinRedirect({ state: { } }).then(function() {
        _management_logger.debug("signinRedirect done")
    }).catch(function(err) {
        _management_logger.error(err)
    })
  } else {
    store_pref("oauth-idp-pending", "true")
    location.href = resource_server.oauth_provider_url
  }
}

function oauth_redirectToHome() {
  let path = get_pref("oauth-return-to")
  clear_pref("oauth-return-to")
  go_to( !path ? "" : path)
}
function go_to(path) {
  location.href = rabbit_path_prefix() + "/" + path
}

function go_to_authority() {
  location.href = oauth.authority
}
function oauth_redirectToLogin(error) {
  if (!error) location.href = rabbit_path_prefix() + "/"
  else {
    location.href = rabbit_path_prefix() + "/?error=" + error
  }
}
export function oauth_completeLogin() {
    mgr.signinRedirectCallback().then(function(user) {
      clear_local_pref(SESSION_EXPIRY);
      set_token_auth(user.access_token);
      oauth_redirectToHome();
    }).catch(function(err) {
      _management_logger.error(err)
      oauth_redirectToLogin(err)
    });
}

export function oauth_initiateLogout() {
  if (oauth.sp_initiated) {
    return mgr.getUser().then(user => {
      if (user != null) {
        mgr.metadataService.getEndSessionEndpoint().then(endpoint => {
          if (endpoint == undefined) {
            // Logout only from management UI
            mgr.removeUser().then(res => {
              clear_auth()
              oauth_redirectToLogin()
            })
          }else {
            // OpenId Connect RP-Initiated Logout
            mgr.signoutRedirect()
          }
        })
      }else {
        clear_auth()
        go_to_home()
      }
    })

  } else {
    go_to_authority()
  }
}

export function oauth_completeLogout() {
    clear_auth()
    mgr.signoutRedirectCallback().then(_ => oauth_redirectToLogin())
}

// oauth2's own login-page renderer and error/logout presentation - the
// implementation behind this module's own oauth2 provider's
// startLoginFlow/presentError/onUnauthorized (registered below).
// start_app_login() (main.js) is called as a bare global rather than
// imported: main.js already imports from auth-providers.js, and this
// module imports from auth-providers.js too, so an explicit import back to
// main.js risks a cycle.
export function startWithOAuthLogin(oauth) {
  if (!oauth.logged_in) {
    hasAnyResourceServerReady(oauth, (oauth, escaped_warnings) => { render_login_oauth(oauth, escaped_warnings); start_app_login(); })
  } else {
    start_app_login()
  }
}

export function render_login_oauth(oauth, escaped_messages) {
  let formatData = {};
  formatData.escaped_warnings = [];
  formatData.notAuthorized = false;
  // What to offer and what to preselect is decided by auth-options.js;
  // the template only renders that decision.
  formatData.auth = authOptions(oauth);
  formatData.auth_options_for_mechanism = auth_options_for_mechanism;
  formatData.auth_section_is_expanded = auth_section_is_expanded;

  if (Array.isArray(escaped_messages)) {
    formatData.escaped_warnings = escaped_messages
  } else if (typeof escaped_messages == "string") {
    formatData.escaped_warnings = [escaped_messages]
    formatData.notAuthorized = escaped_messages == "Not authorized"
  }
  replace_content('outer', format('login_oauth', formatData))

  setup_visibility()
  $('#login').off('click', 'div.section h2, div.section-hidden h2');
  $('#login').on('click', 'div.section h2, div.section-hidden h2', function() {
          toggle_visibility($(this));
      });

  // Bound here, next to the markup they act on, rather than once at
  // startup. replace_content() above builds a fresh #login every time this
  // renders, so handlers scoped to it are discarded with the old element
  // instead of accumulating - which is why these cannot be delegated to
  // document.
  $('#login').on('click', '[data-oauth-action="login"]', function() {
      oauth_initiateLogin($(this).data('resourceId'));
  });
  $('#login').on('click', '[data-oauth-action="logout"]', function() {
      oauth_initiateLogout();
  });
  $('#login').on('submit', '#oauth2-resource-form', function(e) {
      e.preventDefault();
      oauth_initiateLogin(document.getElementById('oauth2-resource').value);
  });
}

export function renderWarningMessageInLoginStatus(oauth, message) {
  render_login_oauth(oauth, message)
}

export function initiate_logout(oauth, error = "") {
    renderWarningMessageInLoginStatus(oauth, error);
}
function validate_openid_configuration(payload) {
  if (payload == null) {
    throw new Error("Payload does not contain openid configuration")
  }
  if (typeof payload.authorization_endpoint != 'string') {
    throw new Error("Missing authorization_endpoint")
  }
  if (typeof payload.token_endpoint != 'string') {
    throw new Error("Missing token_endpoint")
  }
  if (typeof payload.jwks_uri != 'string') {
    throw new Error("Missing jwks_uri")
  }

}

/**
 * Return a warning message for a single OAuth resource already escaped, i.e
 * safe to append to the DOM.
 * @param {*} oauthResource 
 * @param {*} reason 
 * @returns 
 */
function warningMessageOAuthResource(oauthResource, reason) {
  return "OAuth resource [<b>" 
    + fmt_escape_html(oauthResource["label"] != null ? oauthResource.label : oauthResource.id) 
    + "</b>] not available. OpenId Discovery endpoint " 
    + fmt_escape_html(readiness_url(oauthResource)) 
    + fmt_escape_html(reason)
}
/**
 * Return a warning message for multiple OAuth resources already escaped, i.e
 * safe to append to the DOM.
 * @param {*} commonProviderURL 
 * @param {*} oauthResources 
 * @param {*} reason 
 * @returns 
 */
function warningMessageOAuthResources(commonProviderURL, oauthResources, reason) {
  return "OAuth resources [ <b>"
    + oauthResources.map(resource => fmt_escape_html(resource["label"] != null ? resource.label : resource.id)).join("</b>,<b>")
    + "</b>] not available. OpenId Discovery endpoint "
    + fmt_escape_html(commonProviderURL) + fmt_escape_html(reason)
}

export function hasAnyResourceServerReady(oauth, onReadyCallback) {
  // Find out how many distinct oauthServers are configured
  let oauthServers = removeDuplicates(oauth.resource_servers.filter((resource) => resource.sp_initiated))
  oauthServers.forEach(function(entry) { console.log(readiness_url(entry)) })
  if (oauthServers.length > 0) {   // some resources are sp_initiated but there could be idp_initiated too
    Promise.allSettled(oauthServers.map(oauthServer => fetch(readiness_url(oauthServer)).then(res => res.json())))
      .then(results => {
        results.forEach(function(entry) { console.log(entry) })
        let notReadyServers = []
        let notCompliantServers = []

        for (let i = 0; i < results.length; i++) {
          switch (results[i].status) {
            case "fulfilled":
              try {
                validate_openid_configuration(results[i].value)
              }catch(e) {
                console.log("Unable to connect to " + oauthServers[i].oauth_provider_url + ". " + e)
                notCompliantServers.push(oauthServers[i].oauth_provider_url)
              }
              break
            case "rejected":
              notReadyServers.push(oauthServers[i].oauth_provider_url)
              break
          }
        }
        const spOauthServers = oauth.resource_servers.filter((resource) => resource.sp_initiated)
        const groupByProviderURL = spOauthServers.reduce((group, oauthServer) => {
          const { oauth_provider_url } = oauthServer;
          group[oauth_provider_url] = group[oauth_provider_url] ?? [];
          group[oauth_provider_url].push(oauthServer);
          return group;
        }, {})
        let warnings = []
        for(var url in groupByProviderURL){          
          const notReadyResources = groupByProviderURL[url].filter((oauthserver) => notReadyServers.includes(oauthserver.oauth_provider_url))
          const notCompliantResources = groupByProviderURL[url].filter((oauthserver) => notCompliantServers.includes(oauthserver.oauth_provider_url))
          if (notReadyResources.length == 1) {
            warnings.push(warningMessageOAuthResource(notReadyResources[0], " not reachable"))
          }else if (notReadyResources.length > 1) {
            warnings.push(warningMessageOAuthResources(url, notReadyResources, " not reachable"))
          }
          if (notCompliantResources.length == 1) {
            warnings.push(warningMessageOAuthResource(notCompliantResources[0], " not compliant"))
          }else if (notCompliantResources.length > 1) {
            warnings.push(warningMessageOAuthResources(url, notCompliantResources, " not compliant"))
          }
        }
        oauth.declared_resource_servers_count = oauth.resource_servers.length;
        oauth.resource_servers = oauth.resource_servers.filter((resource) =>
          !notReadyServers.includes(resource.oauth_provider_url) && !notCompliantServers.includes(resource.oauth_provider_url));
        oauth.resource_servers.sort((a, b) => a.index - b.index);

        onReadyCallback(oauth, warnings)

      })
  }else {
    onReadyCallback(oauth, [])
  }
}

registerAuthProvider('oauth2', {
    // Populates the global oauth object that login_flow_provider() and
    // authOptions() read - called once at page load, from
    // initializeAuthProviders(), only when this module was actually
    // imported (i.e. oauth2 is configured server-side).
    initialize: function() {
        window.oauth = oauth_initialize_if_required();
    },
    // A failed IDP-initiated login redirects back here with ?error=, which
    // this module set itself on the way out, along with the pending-state
    // markers. Drop those so the next successful login is not redirected
    // somewhere confusing.
    consumeRedirect: function(url) {
        var error = url.searchParams.get('error');
        if (!error) return false;
        clear_pref("oauth-idp-pending");
        clear_pref("oauth-return-to");
        renderWarningMessageInLoginStatus(oauth, fmt_escape_html(error));
        return true;
    },
    startLoginFlow: function() {
        startWithOAuthLogin(oauth);
    },
    // oauth2 has no Sammy route of its own - login is a direct jQuery
    // click/submit handler bound in render_login_oauth(), not a form
    // posted through the app's router.
    registerRoutes: function(sammy) {},
    presentError: function(message) {
        renderWarningMessageInLoginStatus(oauth, message);
    },
    presentSessionExpired: function() {
        this.presentError('Not authorized');
    },
    // presentSessionExpired() re-renders as just a logout button (see
    // login_oauth.ejs's notAuthorized branch) - self-contained via a plain
    // jQuery click handler, no route needed.
    needsRouterAfterSessionExpired: false,
    // No reauthenticate(): logging in again with a raw username/password
    // isn't an operation oauth2 has - its session validity has nothing to
    // do with an internal user record's password field, even if the
    // username happens to coincide. The caller checks for the method's
    // presence rather than calling a no-op, same as registerRoutes().
    signOut: function() {
        clear_auth();
        if (oauth.logged_in) {
            oauth.logged_in = false;
            oauth_initiateLogout();
        } else {
            go_to_home();
        }
    },
    onUnauthorized: function(response, reason) {
        initiate_logout(oauth, reason);
    }
});
