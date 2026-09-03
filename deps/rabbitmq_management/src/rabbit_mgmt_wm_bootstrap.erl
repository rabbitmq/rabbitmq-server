%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_bootstrap).

-export([init/2]).
-include("rabbit_mgmt.hrl").

%%--------------------------------------------------------------------
%% js/bootstrap.js
%% Page bootstrap module for the management UI, generated per-request: it
%% imports basic-auth.js unconditionally, and js/oidc-oauth/helper.js - and
%% therefore the whole oauth2 client library behind it - only when oauth2
%% is actually enabled, inlining the settings that library needs directly
%% rather than through a further import of js/oidc-oauth/bootstrap.js, so
%% loading the page never costs more than one extra request for oauth2.
%% This endpoint only accepts GET method.
%%
%% It can work in conjunction with the /login endpoint. If the users are
%% redirected to the home page of the management ui, and eventually to this
%% endpoint, via the /login endpoint is very likely that the request
%% carries a cookie. It can be the <<"access_token">> cookie or the cookies
%% <<"strict_auth_mechanism">> or <<"preferred_auth_mechanism">>.
%% These cookies are consumed by this endpoint and removed afterwards.
%%
%% Additionally, this endpoint may accept users' authentication mechanism
%% preferences via its corresponding header, in addition to the two cookies
%% mentioned above. But not via request parameters. If this endpoint would
%% have accepted request parameters, it would have to use the "Referer"
%% header to extract the original request parameters. It is possible that
%% in some environments, these headers may be dropped before they reach
%% this endpoint. Therefore, users who can only use request parameters,
%% they have to use the /login endpoint instead.

init(Req0, State) ->
    bootstrap(rabbit_mgmt_headers:set_no_cache_headers(
        rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE), ?MODULE), State).

bootstrap(Req0, State) ->
    AuthSettings0 = rabbit_mgmt_wm_auth:authSettings(),
    {Req1, AuthSettings} = enrich_oauth_settings(Req0, AuthSettings0),
    {Req2, SetTokenAuth} = set_token_auth(AuthSettings, Req1),
    OAuthJs = case proplists:get_value(oauth_enabled, AuthSettings, false) of
        true ->
            Dependencies = rabbit_mgmt_oauth_bootstrap:oauth_dependencies(),
            rabbit_mgmt_oauth_bootstrap:import_dependencies(Dependencies, "./oidc-oauth/helper.js") ++
            rabbit_mgmt_oauth_bootstrap:set_oauth_settings(AuthSettings) ++
            SetTokenAuth ++
            rabbit_mgmt_oauth_bootstrap:export_dependencies(Dependencies);
        false ->
            []
    end,
    JSContent = [
        "import './basic-auth.js';",
        OAuthJs,
        "import { initializeAuthProviders } from './auth-providers.js';",
        "import { check_version } from './main.js';",
        "check_version();",
        "initializeAuthProviders();"
    ],
    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"text/javascript; charset=utf-8">>},
        JSContent, Req2), State}.

enrich_oauth_settings(Req0, AuthSettings) ->
    {Req1, Auth} = get_auth_mechanism(Req0),
    case Auth of
        undefined -> {Req0, AuthSettings};
        _ ->
            ValidAuth = validate_auth_mechanism(Auth, AuthSettings),
            case ValidAuth of
                {preferred_auth_mechanism, Args} -> {Req1, [{preferred_auth_mechanism, Args} | AuthSettings]};
                {strict_auth_mechanism, Args} -> {Req1, [{strict_auth_mechanism, Args} | AuthSettings]};
                {error, _Reason} -> {Req1, AuthSettings}
            end
    end.
get_auth_mechanism(Req) ->
    case get_auth_mechanism_from_cookies(Req) of
        undefined ->
            case cowboy_req:header(<<"x-", ?MANAGEMENT_LOGIN_STRICT_AUTH_MECHANISM/binary>>, Req) of
                undefined ->
                    case cowboy_req:header(<<"x-", ?MANAGEMENT_LOGIN_PREFERRED_AUTH_MECHANISM/binary>>, Req) of
                        undefined -> {Req, undefined};
                        Val -> {Req, {preferred_auth_mechanism, Val}}
                    end;
                Val -> {Req, {strict_auth_mechanism, Val}}
            end;
        {Type, _} = Auth -> { cowboy_req:set_resp_cookie(atom_to_binary(Type),
                                    <<"">>, Req, #{
                                        max_age => 0,
                                        http_only => true,
                                        path => rabbit_mgmt_util:prefixed_path(?OAUTH2_BOOTSTRAP_PATH),
                                        same_site => strict
                                }),
                                Auth
                            }
    end.

get_auth_mechanism_from_cookies(Req) ->
    Cookies = cowboy_req:parse_cookies(Req),
    case proplists:get_value(?MANAGEMENT_LOGIN_STRICT_AUTH_MECHANISM, Cookies) of
        undefined ->
            case proplists:get_value(?MANAGEMENT_LOGIN_PREFERRED_AUTH_MECHANISM, Cookies) of
                undefined -> undefined;
                Val -> {preferred_auth_mechanism, Val}
            end;
        Val -> {strict_auth_mechanism, Val}
    end.
validate_auth_mechanism({Type, <<"oauth2:", Id/binary>>}, AuthSettings) ->
    case maps:is_key(Id, proplists:get_value(oauth_resource_servers, AuthSettings)) of
        true -> {Type, [{type, <<"oauth2">>}, {resource_id, Id}]};
        _ -> {error, {unknown_resource_id, Id}}
    end;
validate_auth_mechanism({Type, <<"basic">>}, _AuthSettings) ->
    {Type, [{type, <<"basic">>}]};
validate_auth_mechanism({_, _}, _AuthSettings) -> {error, unknown_auth_mechanism}.

set_token_auth(AuthSettings, Req0) ->
    case proplists:get_value(oauth_enabled, AuthSettings, false) of
        true ->
            case cowboy_req:parse_header(<<"authorization">>, Req0) of
                {bearer, Token} ->
                    {
                        Req0,
                        ["set_token_auth(", rabbit_json:encode(Token), ");"]
                    };
                _ ->
                    Cookies = cowboy_req:parse_cookies(Req0),
                    case proplists:get_value(?OAUTH2_ACCESS_TOKEN, Cookies) of
                         undefined -> {
                                Req0,
                                []
                            };
                        Token ->
                            {
                                cowboy_req:set_resp_cookie(
                                    ?OAUTH2_ACCESS_TOKEN, <<"">>, Req0, #{
                                        max_age => 0,
                                        http_only => true,
                                        path => rabbit_mgmt_util:prefixed_path(?OAUTH2_BOOTSTRAP_PATH),
                                        same_site => strict
                                    }),
                                ["set_token_auth(", rabbit_json:encode(Token), ");"]
                            }
                    end
            end;
        false -> {
                Req0,
                []
            }
    end.
