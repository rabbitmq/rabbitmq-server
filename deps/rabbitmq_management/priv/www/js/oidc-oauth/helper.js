
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
    if (!resource_server.metadata_url) {
      return resource_server.provider_url + "/.well-known/openid-configuration"
    }else {
      return resource_server.metadata_url
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

  if (authSettings.oauth_resource_servers && Object.keys(authSettings.oauth_resource_servers).length > 0) {

    for (const [resource_server_id, resource_server] of Object.entries(authSettings.oauth_resource_servers)) {
        if (!resource_server.provider_url) {
          resource_server.provider_url = authSettings.oauth_provider_url
        }
        if (!resource_server.provider_url) {
          break
        }
        if (!resource_server.response_type) {
          resource_server.response_type = authSettings.oauth_response_type
          if (!resource_server.response_type) {
            resource_server.response_type = "code"
          }
        }
        if (!resource_server.scopes) {
          resource_server.scopes = authSettings.oauth_scopes
          if (!resource_server.scopes) {
            resource_server.scopes = "openid profile"
          }
        }
        if (!resource_server.client_id) {
          resource_server.client_id = authSettings.oauth_client_id
        }
        if (!resource_server.client_secret) {
          resource_server.client_secret = authSettings.oauth_client_secret
        }
        if (resource_server.initiated_logon_type == "idp_initiated") {
          resource_server.sp_initiated = false
        } else {
          resource_server.sp_initiated = true
        }
        if (!resource_server.metadata_url) {
          resource_server.metadata_url = authSettings.metadata_url
        }
        resource_server.id = resource_server_id
        authSettings.resource_servers.push(resource_server)
    }

  }else if (authSettings.oauth_provider_url) {
    let resource = {
        "provider_url" : authSettings.oauth_provider_url,
        "scopes" : authSettings.oauth_scopes,
        "response_type" : authSettings.oauth_response_type,
        "sp_initiated" : authSettings.oauth_initiated_logon_type == "sp_initiated",
        "id" : authSettings.oauth_resource_id
    }
    if (authSettings.oauth_client_secret) {
      resource.client_secret = authSettings.oauth_client_secret
    }
    if (authSettings.oauth_client_id) {
      resource.client_id = authSettings.oauth_client_id
    }
    if (authSettings.metadata_url) {
      resource.metadata_url = authSettings.metadata_url
    }
    authSettings.resource_servers.push(resource)
  }

  return authSettings;
}


function oauth_initialize_user_manager(resource_server) {
    oidcSettings = {
        //userStore: new WebStorageStateStore({ store: window.localStorage }),
        authority: resource_server.provider_url,
        client_id: resource_server.client_id,
        response_type: resource_server.response_type,
        scope: resource_server.scopes,
        resource: resource_server.id,
        redirect_uri: rabbit_base_uri() + "/js/oidc-oauth/login-callback.html",
        post_logout_redirect_uri: rabbit_base_uri() + "/",

        automaticSilentRenew: true,
        revokeAccessTokenOnSignout: true,
        extraQueryParams: {
          audience: resource_server.id, // required by oauth0
        },
    };
    if (resource_server.client_secret != "") {
      oidcSettings.client_secret = resource_server.client_secret;
    }
    if (resource_server.metadata_url != "") {
      oidcSettings.metadataUrl = resource_server.metadata_url;
    }

    oidc.Log.setLevel(oidc.Log.DEBUG);
    oidc.Log.setLogger(console);

    mgr = new oidc.UserManager(oidcSettings);
    oauth.readiness_url = mgr.settings.metadataUrl;

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
      console.log("addUserLoaded  setting oauth.access_token ")
      oauth.access_token = user.access_token  // DEPRECATED
      set_token_auth(oauth.access_token)
    });

}
function oauth_initialize(authSettings) {
    authSettings = auth_settings_apply_defaults(authSettings);
    oauth = {
      "logged_in": false,
      "enabled" : authSettings.oauth_enabled,
      "resource_servers" : authSettings.resource_servers,
      "oauth_disable_basic_auth" : authSettings.oauth_disable_basic_auth
    }
    if (!oauth.enabled) return oauth;

    resource_server = null

    if (oauth.resource_servers.length == 1) {
      resource_server = oauth.resource_servers[0]
    } else if (has_auth_resource()) {
      resource_server = lookup_resource_server(get_auth_resource())
    }

    if (resource_server) {
      oauth.sp_initiated = resource_server.sp_initiated
      oauth.authority = resource_server.provider_url
      if (!resource_server.sp_initiated) return oauth;
      else oauth_initialize_user_manager(resource_server)
    }

    return oauth;
}

function log() {
    message = ""
    Array.prototype.forEach.call(arguments, function(msg) {
        if (msg instanceof Error) {
            msg = "Error: " + msg.message;
        }
        else if (typeof msg !== "string") {
            msg = JSON.stringify(msg, null, 2);
        }
        message += msg
    });
    _management_logger.info(message)
}

function oauth_is_logged_in() {
    return mgr.getUser().then(user => {
        if (!user) {
            return { "loggedIn": false };
        }
        return { "user": user, "loggedIn": !user.expired };
    });
}
function lookup_resource_server(resource_server_id) {
  let i = 0;

  while (i < oauth.resource_servers.length && oauth.resource_servers[i].id != resource_server_id) {
    i++;
  }
  if (i < oauth.resource_servers.length) return oauth.resource_servers[i]
  else return null
}

function oauth_initiateLogin(resource_server_id) {
  resource_server = lookup_resource_server(resource_server_id)
  if (!resource_server) return;
  set_auth_resource(resource_server_id)

  oauth.sp_initiated = resource_server.sp_initiated
  oauth.authority = resource_server.provider_url

  if (resource_server.sp_initiated) {
    if (!mgr) oauth_initialize_user_manager(resource_server)

    mgr.signinRedirect({ state: { } }).then(function() {
        _management_logger.debug("signinRedirect done")
    }).catch(function(err) {
        _management_logger.error(err)
    })
  } else {
    location.href = resource_server.provider_url
  }
}

function oauth_redirectToHome(oauth) {
  set_token_auth(oauth.access_token)

  path = get_pref("oauth-return-to");
  clear_pref("oauth-return-to")
  go_to(path)
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
function oauth_completeLogin() {
    mgr.signinRedirectCallback().then(user => oauth_redirectToHome(user)).catch(function(err) {
        _management_logger.error(err)
        oauth_redirectToLogin(err)
    });
}

function oauth_initiateLogout() {
  if (oauth.sp_initiated) {
    mgr.signoutRedirect()
  } else {
    go_to_authority()
  }
}
function oauth_completeLogout() {
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
  if (typeof payload.end_session_endpoint != 'string') {
    throw new Error("Missing end_session_endpoint")
  }

}
