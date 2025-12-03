%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_oauth_bootstrap).

-export([init/2]).
-include("rabbit_mgmt.hrl").

%%--------------------------------------------------------------------

init(Req0, State) ->
    bootstrap_oauth(rabbit_mgmt_headers:set_no_cache_headers(
        rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE), ?MODULE), State).

bootstrap_oauth(Req0, State) ->
    AuthSettings = enrich_oauth_settings(Req0, rabbit_mgmt_wm_auth:authSettings()),
    Dependencies = oauth_dependencies(),
    {Req1, SetTokenAuth} = set_token_auth(AuthSettings, Req0),
    JSContent = import_dependencies(Dependencies) ++
                set_oauth_settings(AuthSettings) ++
                SetTokenAuth ++
                export_dependencies(Dependencies),
    
    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"text/javascript; charset=utf-8">>},
        JSContent, Req1), State}.

enrich_oauth_settings(Req0, AuthSettings) ->
    case get_auth_mechanism(Req0) of
        undefined -> AuthSettings;
        {_, _} = Auth -> [Auth | AuthSettings]
    end.
get_auth_mechanism(Req) ->
    case get_param_or_header(<<"strict-auth-mechanism">>, <<"x-strict-auth-mechanism">>, Req) of
        undefined ->
            case get_param_or_header(<<"preferred-auth-mechanism">>, <<"x-preferred-auth-mechanism">>, Req) of
                undefined -> undefined;
                Val -> {preferred_auth_mechanism, Val}
            end;
        Val -> {strict_auth_mechanism, Val}
    end.
get_param_or_header(ParamName, HeaderName, Req) ->
    case rabbit_mgmt_util:qs_val(ParamName, Req) of
        undefined -> cowboy_req:parse_header(HeaderName, Req);
        Val -> Val
    end.

set_oauth_settings(AuthSettings) ->
    JsonAuthSettings = rabbit_json:encode(rabbit_mgmt_format:format_nulls(AuthSettings)),
    ["set_oauth_settings(", JsonAuthSettings, ");"].

set_token_auth(AuthSettings, Req0) ->
    case proplists:get_value(oauth_enabled, AuthSettings, false) of
        true ->
            case cowboy_req:parse_header(<<"authorization">>, Req0) of
                {bearer, Token} -> 
                    {
                        Req0, 
                        ["set_token_auth('", Token, "');"]
                    };
                _ -> 
                    Cookies = cowboy_req:parse_cookies(Req0),
                    case lists:keyfind(?OAUTH2_ACCESS_TOKEN_COOKIE_NAME, 1, Cookies) of 
                        {_, Token} -> 
                            {
                                cowboy_req:set_resp_cookie(
                                    ?OAUTH2_ACCESS_TOKEN_COOKIE_NAME, <<"">>, Req0, #{
                                        max_age => 0,
                                        http_only => true,
                                        path => ?OAUTH2_ACCESS_TOKEN_COOKIE_PATH,
                                        same_site => strict
                                    }), 
                                ["set_token_auth('", Token, "');"]
                            };
                        false -> {
                                Req0, 
                                []
                            }
                    end
            end;
        false -> {
                Req0, 
                []
            }
    end.

import_dependencies(Dependencies) ->
    ["import {", string:join(Dependencies, ","), "} from './helper.js';"].

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
