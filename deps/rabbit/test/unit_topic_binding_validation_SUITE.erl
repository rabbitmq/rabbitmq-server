%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Unit tests for rabbit_exchange_type_topic:validate_binding/2. The
%% queue.bind, exchange.bind and unbind paths are covered by bindings_SUITE.
-module(unit_topic_binding_validation_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [parallel],
      [accepts_up_to_two_hashes,
       rejects_more_than_two_hashes,
       rejects_key_at_maximum_length]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

accepts_up_to_two_hashes(_Config) ->
    %% '#' is a wildcard only when it is the whole word.
    accept([<<>>, <<"a.b.c">>, <<"*.*.*">>, <<"#">>, <<"a.#">>, <<"#.#">>,
            <<"#.a.#">>, <<"a#b">>, <<"##.##.##">>]).

rejects_more_than_two_hashes(_Config) ->
    reject([<<"#.#.#">>, <<"#.*.#.*.#">>, <<"a.#.b.#.c.#">>, <<"#.#.#.#">>]).

%% 128 '#' segments joined by dots occupy 255 bytes, the longest binding key
%% AMQP 0-9-1 can carry.
rejects_key_at_maximum_length(_Config) ->
    Key = iolist_to_binary(lists:join(<<".">>, lists:duplicate(128, <<"#">>))),
    ?assertEqual(255, byte_size(Key)),
    reject([Key]).

accept(Keys) ->
    lists:foreach(fun(K) -> ?assertEqual(ok, validate(K)) end, Keys).

reject(Keys) ->
    lists:foreach(
      fun(K) -> ?assertMatch({error, {binding_invalid, _, _}}, validate(K)) end,
      Keys).

validate(BindingKey) ->
    X = #exchange{name = rabbit_misc:r(<<"/">>, exchange, <<"test">>),
                  type = topic},
    rabbit_exchange_type_topic:validate_binding(X, #binding{key = BindingKey}).
