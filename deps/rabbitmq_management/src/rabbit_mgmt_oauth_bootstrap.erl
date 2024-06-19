%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_oauth_bootstrap).

-export([init/2]).

%%--------------------------------------------------------------------

init(Req0, State) ->
    bootstrap_oauth(rabbit_mgmt_headers:set_no_cache_headers(
        rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE), ?MODULE), State).

bootstrap_oauth(Req0, State) ->
    AuthSettings = rabbit_mgmt_wm_auth:authSettings(),
    Dependencies = oauth_dependencies(),
    JSContent = case proplists:get_value(oauth_enabled, AuthSettings, false) of 
        false -> declare_oauth_initialize_if_required(AuthSettings); 
        true -> import_dependencies(Dependencies) ++ 
                declare_oauth_initialize_if_required(AuthSettings) ++ 
                set_token_auth(Req0) ++ 
                export_dependencies(Dependencies)
                
    end ++ export_dependencies(["oauth_initialize_if_required"]),
    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"text/javascript; charset=utf-8">>}, JSContent, Req0), State}.

declare_oauth_initialize_if_required(AuthSettings) ->
    case proplists:get_value(oauth_enabled, AuthSettings, false) of 
        true ->  ["export default function oauth_initialize_if_required(state) { ",
                "let oauth = oauth_initialize(", rabbit_json:encode(rabbit_mgmt_format:format_nulls(AuthSettings)), "); ", 
                "if (!oauth.enabled) return oauth;"
                "switch (state) { case 'login-callback': oauth_completeLogin(); break; case 'logout-callback': oauth_completeLogout(); break; default: oauth = oauth_initiate(oauth);}",
                "return oauth; }"];
        false -> ["export default function oauth_initialize_if_required(state) { return {oauth_enabled: false}; }"] 
    end.

set_token_auth(Req0) ->
    case application:get_env(rabbitmq_management, oauth_enabled, false) of
        true ->
            case cowboy_req:parse_header(<<"authorization">>, Req0) of
                {bearer, Token} ->  ["set_token_auth('", Token, "');"];
                _ -> []
            end;
        false -> []
    end.

import_dependencies(Dependencies) ->
    ["import {", string:join(Dependencies, ","), "} from './helper.js';"].

oauth_dependencies() ->
    ["hasAnyResourceServerReady", "oauth_initialize", "oauth_initiate", "oauth_initiateLogin", "oauth_initiateLogout", "oauth_completeLogin", "oauth_completeLogout"].

export_dependencies(Dependencies) ->
    [ io_lib:format("window.~s = ~s;", [Dep, Dep]) || Dep <- Dependencies ].
