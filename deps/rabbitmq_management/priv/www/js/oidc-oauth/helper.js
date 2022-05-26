
//import {UserManager} from "./scripts/oidc-client-ts/oidc-client-ts.js"
// import {axios} from "./scripts/axios/axios.min.js"


var mgr;

function oauth_initialize_if_required() {
    rabbit_port = window.location.port ? ":" +  window.location.port : ""
    rabbit_base_uri = window.location.protocol + "//" + window.location.hostname + rabbit_port

    var request = new XMLHttpRequest();
    request.open('GET', rabbit_base_uri + '/api/auth', false);
    request.send(null);
    if (request.status === 200) {
        return oauth_initialize(JSON.parse(request.responseText));
    }else {
        return { "enable" : false };
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
      "enable" : authSettings.oauth_enable,
      "authority" : authSettings.oauth_provider_url
    }

    if (!oauth.enable) return oauth;

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
        post_logout_redirect_uri: rabbit_base_uri + "/js/oidc-oauth/logout-callback.html",

        filterProtocolClaims: true,
        automaticSilentRenew: true,
        revokeAccessTokenOnSignout: true,
    };
    if (authSettings.oauth_metadata_url != "") oidcSettings.metadataUrl = authSettings.oauth_metadata_url

    if (authSettings.enable_uaa == true) {
      // This is required for old versions of UAA because the newer ones do expose
      // the end_session_endpoint on the oidc discovery endpoint, .a.k.a. metadataUrl
      oidcSettings.metadataSeed = {
        end_session_endpoint: authSettings.oauth_provider_url + '/logout.do'
      }
    }

    mgr = new oidc.UserManager(oidcSettings);
    oauth.readiness_url = mgr.settings.metadataUrl

    oidc.Log.setLogger(console);
    oidc.Log.setLevel(oidc.Log.INFO);

    mgr.events.addAccessTokenExpiring(function() {
        console.log("token expiring...");
    });
      mgr.events.addAccessTokenExpired(function() {
          console.log("token expired !!");
      });
    mgr.events.addSilentRenewError(function(err) {
        console.log("token expiring failed due to " + err);
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
    console.log(message)
}




function oauth_registerCallbacks() {
  mgr.events.addUserLoaded(function (user) {
      console.log("addUserLoaded=> ", user);
      mgr.getUser().then(function() {
          console.log("getUser loaded user after userLoaded event fired");
      });
  });
  mgr.events.addUserUnloaded(function (e) {
      console.log("addUserUnloaded=> ", e);
  });

  mgr.events.addUserSignedIn(function (e) {
      log("addUserSignedIn=> " , e);
  });
  mgr.events.addUserSignedOut(function (e) {
      log("addUserSignedOut=> ", e);
  });

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
    mgr.signinRedirect({ state: { foo: "bar" } /*, useReplaceToNavigate: true*/ }).then(function() {
        log("signinRedirect done");
    }).catch(function(err) {
        console.error(err);
        log(err);
    });
}
function oauth_redirectToHome(oauth) {
  set_auth_pref(oauth.user_name + ':' + oauth.access_token);
  location.href = "/"
}
function oauth_redirectToLogin(error) {
  if (!error) location.href = "/"
  else {
    location.href = "/?error=" + error
  }
}
function oauth_completeLogin() {
    mgr.signinRedirectCallback().then(user => oauth_redirectToHome(user)).catch(function(err) {
        console.error(err);
        log(err);
        oauth_redirectToLogin(err)
    });
}

function oauth_initiateLogout() {
    mgr.signoutRedirect();
}
function oauth_completeLogout() {
    mgr.signoutRedirectCallback().then(_ => oauth_redirectToLogin());
}
