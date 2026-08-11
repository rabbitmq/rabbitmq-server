%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_oauth_token_proxy_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     inject_client_secret_when_absent,
     does_not_override_client_secret_when_present,
     rewrite_replaces_only_token_endpoint,
     external_authority_no_headers,
     external_authority_proto_only,
     external_authority_host_only,
     external_authority_host_with_port,
     external_authority_all_headers,
     external_authority_multiple_proxies
    ].

inject_client_secret_when_absent(_Config) ->
    Params = [{<<"grant_type">>, <<"authorization_code">>},
              {<<"code">>, <<"abc">>}],
    Result = rabbit_mgmt_oauth_token_proxy:inject_client_secret(Params, <<"s3cret">>),
    ?assertEqual(<<"s3cret">>,
                 proplists:get_value(<<"client_secret">>, Result)),
    ?assertEqual(<<"abc">>, proplists:get_value(<<"code">>, Result)).

does_not_override_client_secret_when_present(_Config) ->
    Params = [{<<"grant_type">>, <<"refresh_token">>},
              {<<"client_secret">>, <<"from-client">>}],
    Result = rabbit_mgmt_oauth_token_proxy:inject_client_secret(Params, <<"s3cret">>),
    ?assertEqual(<<"from-client">>,
                 proplists:get_value(<<"client_secret">>, Result)).

rewrite_replaces_only_token_endpoint(_Config) ->
    Metadata = rabbit_json:encode(#{
        <<"issuer">> => <<"https://idp">>,
        <<"authorization_endpoint">> => <<"https://idp/authorize">>,
        <<"token_endpoint">> => <<"https://idp/token">>,
        <<"jwks_uri">> => <<"https://idp/keys">>}),
    Proxy = <<"https://rabbit/js/oidc-oauth/token-endpoint/rabbitmq">>,
    Rewritten = rabbit_json:decode(
        rabbit_mgmt_oauth_token_proxy:rewrite_token_endpoint(Metadata, Proxy)),
    ?assertEqual(Proxy, maps:get(<<"token_endpoint">>, Rewritten)),
    ?assertEqual(<<"https://idp/authorize">>,
                 maps:get(<<"authorization_endpoint">>, Rewritten)),
    ?assertEqual(<<"https://idp/keys">>, maps:get(<<"jwks_uri">>, Rewritten)).

external_authority_no_headers(_Config) ->
    ?assertEqual([<<"http">>, "://", <<"backend">>, [":", <<"15672">>]],
                 rabbit_mgmt_oauth_token_proxy:external_authority(<<"http">>, <<"backend">>, 15672, undefined, undefined, undefined)).

external_authority_proto_only(_Config) ->
    ?assertEqual([<<"https">>, "://", <<"backend">>, ""],
                 rabbit_mgmt_oauth_token_proxy:external_authority(<<"http">>, <<"backend">>, 15672, <<"https">>, undefined, undefined)).

external_authority_host_only(_Config) ->
    ?assertEqual([<<"http">>, "://", <<"proxy.com">>, [":", <<"15672">>]],
                 rabbit_mgmt_oauth_token_proxy:external_authority(<<"http">>, <<"backend">>, 15672, undefined, <<"proxy.com">>, undefined)).

external_authority_host_with_port(_Config) ->
    ?assertEqual([<<"https">>, "://", <<"proxy.com">>, [":", <<"8443">>]],
                 rabbit_mgmt_oauth_token_proxy:external_authority(<<"http">>, <<"backend">>, 15672, <<"https">>, <<"proxy.com:8443">>, undefined)),
    ?assertEqual([<<"https">>, "://", <<"[::1]">>, [":", <<"8443">>]],
                 rabbit_mgmt_oauth_token_proxy:external_authority(<<"http">>, <<"backend">>, 15672, <<"https">>, <<"[::1]:8443">>, undefined)).

external_authority_all_headers(_Config) ->
    ?assertEqual([<<"https">>, "://", <<"proxy.com">>, [":", <<"8443">>]],
                 rabbit_mgmt_oauth_token_proxy:external_authority(<<"http">>, <<"backend">>, 15672, <<"https">>, <<"proxy.com">>, <<"8443">>)),
    %% Forwarded port overrides host port
    ?assertEqual([<<"https">>, "://", <<"proxy.com">>, [":", <<"8443">>]],
                 rabbit_mgmt_oauth_token_proxy:external_authority(<<"http">>, <<"backend">>, 15672, <<"https">>, <<"proxy.com:443">>, <<"8443">>)).

external_authority_multiple_proxies(_Config) ->
    ?assertEqual([<<"https">>, "://", <<"proxy.com">>, ""],
                 rabbit_mgmt_oauth_token_proxy:external_authority(<<"http">>, <<"backend">>, 15672, <<"https, http">>, <<"proxy.com, other.com">>, undefined)).
