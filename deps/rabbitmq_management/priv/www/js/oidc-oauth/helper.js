
var mgr;
var _management_logger;

function oauth_initialize_if_required() {
    rabbit_port = window.location.port ? ":" +  window.location.port : ""
    rabbit_path_prefix = window.location.pathname.replace(/(\/js\/oidc-oauth\/.*$|\/+$)/, "")
    rabbit_base_uri = window.location.protocol + "//" + window.location.hostname
      + rabbit_port + rabbit_path_prefix

    var request = new XMLHttpRequest();
    request.open("GET", rabbit_base_uri + "/api/auth", false);
    request.send(null);
    if (request.status === 200) {
        return oauth_initialize(JSON.parse(request.responseText));
    } else {
        return { "enabled" : false };
    }

}


function auth_settings_apply_defaults(authSettings) {
  if (authSettings.enable_uaa == "true") {

    if (!authSettings.oauth_provider_url) {
      authSettings.oauth_provider_url = authSettings.uaa_location
    }
    if (!authSettings.oauth_client_id) {
      authSettings.oauth_client_id = authSettings.uaa_client_id
    }
    if (!authSettings.oauth_client_secret) {
      authSettings.oauth_client_secret = authSettings.uaa_client_secret
    }
    if (!authSettings.oauth_scopes) {
      authSettings.oauth_scopes = "openid profile " + authSettings.oauth_resource_id + ".*";
    }
  }
  if (!authSettings.oauth_response_type) {
    authSettings.oauth_response_type = "code"; // although the default value in oidc client
  }

  if (!authSettings.oauth_scopes) {
    authSettings.oauth_scopes = "openid profile";
  }

  return authSettings;

}


function oauth_initialize(authSettings) {
    oauth = {
      "logged_in": false,
      "enabled" : authSettings.oauth_enabled,
      "authority" : authSettings.oauth_provider_url
    }

    if (!oauth.enabled) return oauth;

    authSettings = auth_settings_apply_defaults(authSettings);

    oidcSettings = {
        //userStore: new WebStorageStateStore({ store: window.localStorage }),
        authority: authSettings.oauth_provider_url,
        client_id: authSettings.oauth_client_id,
        client_secret: authSettings.oauth_client_secret,
        response_type: authSettings.oauth_response_type,
        scope: authSettings.oauth_scopes, // for uaa we may need to include <resource-server-id>.*
        resource: authSettings.oauth_resource_id,
        redirect_uri: rabbit_base_uri + "/js/oidc-oauth/login-callback.html",
        post_logout_redirect_uri: rabbit_base_uri + "/",

        automaticSilentRenew: true,
        revokeAccessTokenOnSignout: true,
        extraQueryParams: {
          audience: authSettings.oauth_resource_id, // required by oauth0
        },
    };
    if (authSettings.oauth_metadata_url != "") oidcSettings.metadataUrl = authSettings.oauth_metadata_url

    if (authSettings.enable_uaa == true) {
      // This is required for old versions of UAA because the newer ones do expose
      // the end_session_endpoint on the oidc discovery endpoint, .a.k.a. metadataUrl
      oidcSettings.metadataSeed = {
        end_session_endpoint: authSettings.oauth_provider_url + "/logout.do"
      }
    }
    oidc.Log.setLevel(oidc.Log.DEBUG);
    oidc.Log.setLogger(console);

    mgr = new oidc.UserManager(oidcSettings);
    oauth.readiness_url = mgr.settings.metadataUrl

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
        oauth.access_token = user.access_token;
     });

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
function oauth_initiateLogin() {
    mgr.signinRedirect({ state: { } }).then(function() {
        _management_logger.debug("signinRedirect done");
    }).catch(function(err) {
        _management_logger.error(err);
    });
}

function oauth_redirectToHome(oauth) {
  set_auth_pref(oauth.user_name + ":" + oauth.access_token);
  location.href = rabbit_path_prefix + "/"
}
function oauth_redirectToLogin(error) {
  _management_logger.debug("oauth_redirectToLogin called");
  if (!error) location.href = rabbit_path_prefix + "/"
  else {
    location.href = rabbit_path_prefix + "/?error=" + error
  }
}
function oauth_completeLogin() {
    mgr.signinRedirectCallback().then(user => oauth_redirectToHome(user)).catch(function(err) {
        _management_logger.error(err);
        oauth_redirectToLogin(err)
    });
}

function oauth_initiateLogout() {
    mgr.signoutRedirect();
}
function oauth_completeLogout() {
    mgr.signoutRedirectCallback().then(_ => oauth_redirectToLogin());
}
