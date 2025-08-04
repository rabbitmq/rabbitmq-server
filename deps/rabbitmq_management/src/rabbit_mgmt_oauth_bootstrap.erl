%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_oauth_bootstrap).

-export([init/2]).
-include("rabbit_mgmt.hrl").
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------

init(Req0, State) ->
    bootstrap_oauth(rabbit_mgmt_headers:set_no_cache_headers(
        rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE), ?MODULE), State).

bootstrap_oauth(Req0, State) ->
    AuthSettings = rabbit_mgmt_wm_auth:authSettings(),
    Dependencies = oauth_dependencies(),
    case set_token_auth(AuthSettings, Req0) of 
        {error, Reason} -> 
            rabbit_mgmt_util:not_authorised(Reason, Req0, State);
        {Req1, SetTokenAuth} ->
            JSContent = import_dependencies(Dependencies) ++
                        set_oauth_settings(AuthSettings) ++
                        SetTokenAuth ++
                        export_dependencies(Dependencies),
            
            {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"text/javascript; charset=utf-8">>},
                JSContent, Req1), State}
    end.

set_oauth_settings(AuthSettings) ->
    JsonAuthSettings = rabbit_json:encode(rabbit_mgmt_format:format_nulls(AuthSettings)),
    ["set_oauth_settings(", JsonAuthSettings, ");"].

set_token_auth(AuthSettings, Req0) ->
    TokenOrError = case proplists:get_value(oauth_enabled, AuthSettings, false) of
        true ->
            case cowboy_req:parse_header(<<"authorization">>, Req0) of
                {bearer, Token} -> 
                    ?LOG_DEBUG("set_token_auth bearer token ~p", [Token]),                    
                    {
                        Req0, 
                        Token
                    };
                _ -> 
                    Cookies = cowboy_req:parse_cookies(Req0),
                    case lists:keyfind(?OAUTH2_ACCESS_TOKEN_COOKIE_NAME, 1, Cookies) of 
                        {_, Token} ->                             
                            ?LOG_DEBUG("set_token_auth cookie token ~p", [Token]),
                            {
                                cowboy_req:set_resp_cookie(
                                    ?OAUTH2_ACCESS_TOKEN_COOKIE_NAME, <<"">>, Req0, #{
                                        max_age => 0,
                                        http_only => true,
                                        path => ?OAUTH2_ACCESS_TOKEN_COOKIE_PATH,
                                        same_site => strict
                                    }), 
                                Token
                            };
                        false -> {
                                Req0, 
                                undefined
                            }
                    end
            end;
        false -> {
                Req0, 
                undefined
            }
    end,
    case TokenOrError of 
        {error, _} = Error -> Error;
        {Req, undefined} -> {Req, []};
        {Req, Tk} ->
            case oauth2_client:is_jwt_token(Tk) of 
                true -> 
                    {
                        Req0, 
                        ["set_token_auth('", Tk, "');"]
                    };
                false -> 
                    case map_opaque_to_jwt_token(Tk) of
                        {ok, Tk1} -> 
                            ?LOG_DEBUG("Successfully introspected token : ~p", [Tk1]),
                            {
                                Req0, 
                                ["set_token_auth('", Tk1, "');"]
                            };
                        {error, _} = Err1 -> 
                            Err1
                    end      
            end
    end.


map_opaque_to_jwt_token(OpaqueToken) ->
    case oauth2_client:introspect_token(OpaqueToken) of 
        {error, introspected_token_not_valid} = Error -> Error;        
        {ok, JwtPayload} -> 
            case oauth2_client:sign_token(JwtPayload) of 
                {ok, JWT} -> {ok, JWT};
                {error, _} = Err1 -> Err1
            end
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
