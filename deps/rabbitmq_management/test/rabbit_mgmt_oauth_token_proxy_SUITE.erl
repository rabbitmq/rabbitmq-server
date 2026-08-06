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
     authority_uses_connection_when_not_forwarded,
     authority_honours_forwarded_proto,
     authority_honours_forwarded_port,
     authority_takes_first_forwarded_value
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

authority_uses_connection_when_not_forwarded(_Config) ->
    ?assertEqual(<<"http://rabbit">>,
                 authority(<<"http">>, <<"rabbit">>, 80, undefined, undefined)),
    ?assertEqual(<<"https://rabbit">>,
                 authority(<<"https">>, <<"rabbit">>, 443, undefined, undefined)),
    ?assertEqual(<<"http://rabbit:15672">>,
                 authority(<<"http">>, <<"rabbit">>, 15672, undefined, undefined)).

authority_honours_forwarded_proto(_Config) ->
    ?assertEqual(<<"https://rabbit">>,
                 authority(<<"http">>, <<"rabbit">>, 80, <<"https">>, undefined)),
    ?assertEqual(<<"https://rabbit">>,
                 authority(<<"http">>, <<"rabbit">>, 15672, <<"https">>, undefined)).

authority_honours_forwarded_port(_Config) ->
    ?assertEqual(<<"https://rabbit:8443">>,
                 authority(<<"http">>, <<"rabbit">>, 80, <<"https">>, <<"8443">>)),
    ?assertEqual(<<"https://rabbit">>,
                 authority(<<"http">>, <<"rabbit">>, 80, <<"https">>, <<"443">>)).

authority_takes_first_forwarded_value(_Config) ->
    ?assertEqual(<<"https://rabbit">>,
                 authority(<<"http">>, <<"rabbit">>, 80,
                           <<"https, http">>, undefined)).

authority(ConnScheme, Host, ConnPort, ForwardedProto, ForwardedPort) ->
    iolist_to_binary(
      rabbit_mgmt_oauth_token_proxy:external_authority(
        ConnScheme, Host, ConnPort, ForwardedProto, ForwardedPort)).
