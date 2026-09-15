%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_oauth_bootstrap).

-export([init/2, oauth_dependencies/0, import_dependencies/2, set_oauth_settings/1,
         export_dependencies/1]).
-include("rabbit_mgmt.hrl").

%%--------------------------------------------------------------------
%% js/oidc-oauth/bootstrap.js
%% Seeds js/oidc-oauth/helper.js's oauth2 settings for pages that embed it
%% directly - the oidc login/logout callback pages - rather than through
%% js/bootstrap.js's own inlined copy of this same logic (see
%% rabbit_mgmt_wm_bootstrap.erl). Those pages never went through /login's
%% redirect-home-with-a-cookie flow, so there is no cookie-based auth
%% mechanism preference or access token to resolve here.
%% This endpoint only accepts GET method.

init(Req0, State) ->
    bootstrap_oauth(rabbit_mgmt_headers:set_no_cache_headers(
        rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE), ?MODULE), State).

bootstrap_oauth(Req0, State) ->
    AuthSettings = rabbit_mgmt_wm_auth:authSettings(),
    Dependencies = oauth_dependencies(),
    JSContent = import_dependencies(Dependencies, "./helper.js") ++
                set_oauth_settings(AuthSettings) ++
                export_dependencies(Dependencies),

    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"text/javascript; charset=utf-8">>},
        JSContent, Req0), State}.

set_oauth_settings(AuthSettings) ->
    JsonAuthSettings = rabbit_json:encode(rabbit_mgmt_format:prepare_for_encoding(AuthSettings)),
    ["set_oauth_settings(", JsonAuthSettings, ");"].

%% HelperPath is relative to whichever page is generated: "./helper.js" from
%% this module's own js/oidc-oauth/bootstrap.js, "./oidc-oauth/helper.js"
%% from js/bootstrap.js.
import_dependencies(Dependencies, HelperPath) ->
    ["import {", string:join(Dependencies, ","), "} from '", HelperPath, "';"].

oauth_dependencies() ->
    ["oauth_initialize_if_required",
        "hasAnyResourceServerReady",
        "oauth_initialize", "oauth_initiate",
        "oauth_initiateLogin",
        "oauth_initiateLogout",
        "oauth_completeLogin",
        "oauth_completeLogout",
        "set_oauth_settings"].

export_dependencies(Dependencies) ->
    [ io_lib:format("window.~s = ~s;", [Dep, Dep]) || Dep <- Dependencies ].
