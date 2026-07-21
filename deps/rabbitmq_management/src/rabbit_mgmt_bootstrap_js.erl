-module(rabbit_mgmt_bootstrap_js).
-export([init/2]).

init(Req0, State) ->
    Req1 = rabbit_mgmt_headers:set_no_cache_headers(
               rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE), ?MODULE),
    
    AuthSettings = rabbit_mgmt_wm_auth:authSettings(),
    Imports = generate_imports(AuthSettings),
    Body = generate_body(AuthSettings),
    
    JSContent = [
        "// Dynamically generated bootstrap.js\n",
        Imports,
        "\n",
        Body
    ],
    
    Req2 = cowboy_req:reply(200, #{<<"content-type">> => <<"application/javascript; charset=utf-8">>},
                            iolist_to_binary(JSContent), Req1),
    {ok, Req2, State}.

generate_imports(AuthSettings) ->
    OauthImport = case proplists:get_value(oauth_enabled, AuthSettings, false) of
        true -> "import './oidc-oauth/bootstrap.js';\nimport { oauth_initialize_if_required } from './oidc-oauth/helper.js';\n";
        false -> ""
    end,
    SessionImport = case rabbit_mgmt_features:is_sessions_enabled() of
        true -> "import './session.js';\n";
        false -> ""
    end,
    [OauthImport, SessionImport].

generate_body(AuthSettings) ->
    OauthInit = case proplists:get_value(oauth_enabled, AuthSettings, false) of
        true -> "window.oauth = oauth_initialize_if_required();\n";
        false -> "window.oauth = {enabled: false};\n"
    end,
    SessionInit = case rabbit_mgmt_features:is_sessions_enabled() of
        true -> "";
        false -> "window.check_session = function() { return true; };\nwindow.clear_session = function() {};\nwindow.start_session_heartbeat = function() {};\n"
    end,
    [
        "check_version();\n",
        OauthInit,
        SessionInit
    ].