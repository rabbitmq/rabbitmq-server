%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_oauth_token_proxy_prop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-compile([export_all, nowarn_export_all]).

-define(NUM_TESTS, 500).
-define(CONNECTION, #{scheme => <<"http">>,
                      host => <<"backend">>,
                      port => 15672}).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], [
         authority_is_always_a_well_formed_origin_prop,
         rightmost_forwarded_host_wins_prop
     ]}
    ].

authority_is_always_a_well_formed_origin_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_authority_is_always_a_well_formed_origin/0, [], ?NUM_TESTS).

rightmost_forwarded_host_wins_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_rightmost_forwarded_host_wins/0, [], ?NUM_TESTS).

prop_authority_is_always_a_well_formed_origin() ->
    ?FORALL(Forwarded, forwarded_headers(),
            begin
                Authority = authority(Forwarded),
                case uri_string:parse(Authority) of
                    #{scheme := Scheme, host := Host, path := <<>>} = Parsed ->
                        lists:member(Scheme, [<<"http">>, <<"https">>])
                            andalso Host =/= <<>>
                            andalso valid_port(maps:get(port, Parsed, undefined))
                            andalso not is_map_key(userinfo, Parsed)
                            andalso not is_map_key(query, Parsed)
                            andalso not is_map_key(fragment, Parsed);
                    _ ->
                        false
                end
            end).

prop_rightmost_forwarded_host_wins() ->
    ?FORALL({Left, Right}, {host_value(), host_value()},
            authority(#{host => <<Left/binary, ", ", Right/binary>>}) =:=
                authority(#{host => Right})).

valid_port(undefined) -> true;
valid_port(Port) -> Port > 0 andalso Port < 65536.

authority(Forwarded) ->
    iolist_to_binary(
      rabbit_mgmt_oauth_token_proxy:external_authority(?CONNECTION, Forwarded)).

forwarded_headers() ->
    ?LET({Proto, Host, Port},
         {optional(header_value()), optional(header_value()),
          optional(header_value())},
         maps:filter(fun(_, Value) -> Value =/= undefined end,
                     #{proto => Proto, host => Host, port => Port})).

optional(Type) ->
    oneof([undefined, Type]).

header_value() ->
    oneof([binary(),
           elements([<<>>, <<"https">>, <<"HTTPS">>, <<"javascript">>,
                     <<"proxy.com">>, <<"proxy.com:8443">>, <<"[::1]">>,
                     <<"[::1]:8443">>, <<"[::1">>, <<"evil.com/x">>,
                     <<"evil.com?a=b">>, <<"evil.com#f">>,
                     <<"good.com@evil.com">>, <<"a.com:xx">>, <<"a b">>,
                     <<"8443">>, <<"abc">>, <<"0">>, <<"99999">>,
                     <<"evil.com, proxy.com">>, <<" https , http ">>])]).

host_value() ->
    elements([<<"proxy.com">>, <<"a.example">>, <<"h.example:8443">>,
              <<"[::1]">>, <<"[::1]:8443">>, <<>>, <<"evil.com/x">>]).
