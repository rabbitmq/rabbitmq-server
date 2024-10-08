import {oidc} from './oidc-client-ts.3.0.1.min.js';

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
        if (!resource_server.oauth_client_secret && resource_server.oauth_client_id
            && authSettings.oauth_client_secret) {
          resource_server.oauth_client_secret = authSettings.oauth_client_secret
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
      oauth = oauth_initiate(oauth);
  }
  return oauth;
}

export function oauth_initiate(oauth) {
  if (oauth.enabled) {
    if (!oauth.sp_initiated) {
        oauth.logged_in = has_auth_credentials();
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
  if (resource_server.oauth_client_secret != "") {
    oidcSettings.client_secret = resource_server.oauth_client_secret
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
      "oauth_disable_basic_auth" : authSettings.oauth_disable_basic_auth
    }
    if (!oauth.enabled) return oauth;

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
      set_token_auth(user.access_token);
      oauth_redirectToHome();
    }).catch(function(err) {
      _management_logger.error(err)
      oauth_redirectToLogin(err)
    });
}

export function oauth_initiateLogout() {
  if (oauth.sp_initiated) {
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
  } else {
    go_to_authority()
  }
}

export function oauth_completeLogout() {
    clear_auth()
    mgr.signoutRedirectCallback().then(_ => oauth_redirectToLogin())
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

function warningMessageOAuthResource(oauthResource, reason) {
  return "OAuth resource [<b>" + (oauthResource["label"] != null ? oauthResource.label : oauthResource.id) +
    "</b>] not available. OpenId Discovery endpoint " + readiness_url(oauthResource) + reason
}
function warningMessageOAuthResources(commonProviderURL, oauthResources, reason) {
  return "OAuth resources [ <b>" + oauthResources.map(resource => resource["label"] != null ? resource.label : resource.id).join("</b>,<b>")
    + "</b>] not available. OpenId Discovery endpoint " + commonProviderURL + reason
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
          console.log(url + ': ' + groupByProviderURL[url]);
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
        console.log("warnings:" + warnings)
        oauth.declared_resource_servers_count = oauth.resource_servers.length
        oauth.resource_servers = oauth.resource_servers.filter((resource) =>
          !notReadyServers.includes(resource.oauth_provider_url) && !notCompliantServers.includes(resource.oauth_provider_url))

          onReadyCallback(oauth, warnings)

      })
  }else {
    onReadyCallback(oauth, [])
  }
}
