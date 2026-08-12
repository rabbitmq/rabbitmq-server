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

-define(CONNECTION, #{scheme => <<"http">>,
                      host => <<"backend">>,
                      port => 15672}).

all() ->
    [
     inject_client_secret_when_absent,
     does_not_override_client_secret_when_present,
     rewrite_replaces_only_token_endpoint,
     authority_without_forwarded_headers_describes_the_connection,
     forwarded_proto_selects_the_scheme_and_its_default_port,
     forwarded_host_may_carry_the_port,
     forwarded_port_wins_over_the_port_in_the_forwarded_host,
     forwarded_ipv6_host_keeps_its_brackets,
     connection_ipv6_host_keeps_its_brackets,
     rightmost_forwarded_value_wins,
     unusable_forwarded_scheme_falls_back_to_the_connection,
     unusable_forwarded_host_falls_back_to_the_connection,
     unusable_forwarded_port_falls_back_to_the_connection,
     invalid_utf8_forwarded_values_fall_back_to_the_connection,
     out_of_range_port_in_the_forwarded_host_is_ignored,
     forwarded_host_without_a_forwarded_proto_keeps_the_listener_port
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

authority_without_forwarded_headers_describes_the_connection(_Config) ->
    ?assertEqual(<<"http://backend:15672">>, authority(#{})).

forwarded_proto_selects_the_scheme_and_its_default_port(_Config) ->
    ?assertEqual(<<"https://backend">>, authority(#{proto => <<"https">>})),
    ?assertEqual(<<"https://proxy.com">>,
                 authority(#{proto => <<"https">>, host => <<"proxy.com">>})),
    ?assertEqual(<<"https://proxy.com">>,
                 authority(#{proto => <<"HTTPS">>, host => <<"proxy.com">>})).

forwarded_host_may_carry_the_port(_Config) ->
    ?assertEqual(<<"https://proxy.com:8443">>,
                 authority(#{proto => <<"https">>, host => <<"proxy.com:8443">>})).

forwarded_port_wins_over_the_port_in_the_forwarded_host(_Config) ->
    ?assertEqual(<<"https://proxy.com:8443">>,
                 authority(#{proto => <<"https">>, host => <<"proxy.com:443">>,
                             port => <<"8443">>})).

forwarded_ipv6_host_keeps_its_brackets(_Config) ->
    ?assertEqual(<<"https://[::1]:8443">>,
                 authority(#{proto => <<"https">>, host => <<"[::1]:8443">>})),
    ?assertEqual(<<"https://[::1]">>,
                 authority(#{proto => <<"https">>, host => <<"[::1]">>})).

%% cowboy_req:host/1 returns an IPv6 literal already bracketed.
connection_ipv6_host_keeps_its_brackets(_Config) ->
    Connection = #{scheme => <<"http">>, host => <<"[::1]">>, port => 15672},
    ?assertEqual(<<"http://[::1]:15672">>,
                 iolist_to_binary(
                   rabbit_mgmt_oauth_token_proxy:external_authority(Connection, #{}))).

rightmost_forwarded_value_wins(_Config) ->
    ?assertEqual(<<"https://proxy.com">>,
                 authority(#{proto => <<"http, https">>,
                             host => <<"evil.example.com, proxy.com">>})),
    ?assertEqual(<<"https://proxy.com:8443">>,
                 authority(#{proto => <<"https">>, host => <<"proxy.com">>,
                             port => <<"1, 8443">>})).

unusable_forwarded_scheme_falls_back_to_the_connection(_Config) ->
    ?assertEqual(<<"http://proxy.com:15672">>,
                 authority(#{proto => <<"javascript">>, host => <<"proxy.com">>})),
    ?assertEqual(<<"http://proxy.com:15672">>,
                 authority(#{proto => <<>>, host => <<"proxy.com">>})).

unusable_forwarded_host_falls_back_to_the_connection(_Config) ->
    Unusable = [<<>>, <<"evil.com/x">>, <<"evil.com?a=b">>, <<"evil.com#f">>,
                <<"good.com@evil.com">>, <<"a.com:xx">>, <<"a b">>, <<"[::1">>],
    [?assertEqual(<<"https://backend">>,
                  authority(#{proto => <<"https">>, host => Host}))
     || Host <- Unusable].

unusable_forwarded_port_falls_back_to_the_connection(_Config) ->
    [?assertEqual(<<"http://backend:15672">>, authority(#{port => Port}))
     || Port <- [<<>>, <<"abc">>, <<"0">>, <<"99999">>]].

out_of_range_port_in_the_forwarded_host_is_ignored(_Config) ->
    [?assertEqual(<<"https://proxy.com">>,
                  authority(#{proto => <<"https">>, host => Host}))
     || Host <- [<<"proxy.com:99999">>, <<"proxy.com:0">>]].

%% The listener port is not the one the browser used, but without a
%% trusted-proxy setting nothing better can be inferred.
forwarded_host_without_a_forwarded_proto_keeps_the_listener_port(_Config) ->
    ?assertEqual(<<"http://proxy.com:15672">>,
                 authority(#{host => <<"proxy.com">>})).

invalid_utf8_forwarded_values_fall_back_to_the_connection(_Config) ->
    [?assertEqual(<<"http://backend:15672">>, authority(Forwarded))
     || Forwarded <- [#{proto => <<128>>}, #{host => <<128>>},
                      #{port => <<128>>}, #{host => <<"a", 255, ".com">>}]].

authority(Forwarded) ->
    iolist_to_binary(
      rabbit_mgmt_oauth_token_proxy:external_authority(?CONNECTION, Forwarded)).
