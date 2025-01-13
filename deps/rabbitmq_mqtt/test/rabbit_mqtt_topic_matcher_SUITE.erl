%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
-module(rabbit_mqtt_topic_matcher_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("rabbitmq_mqtt/include/rabbit_mqtt_topic_matcher.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, tests}].

groups() ->
    [{tests,
      [parallel],
      [new, add, remove, match, pattern_is_matching, match_iter, clear, multiple_patterns]}].

new(_Config) ->
    Globber = rabbit_mqtt_topic_matcher:new(),
    ?assertEqual(#globber{}, Globber),
    Globber2 = rabbit_mqtt_topic_matcher:new(<<".">>, <<"+">>, <<"#">>),
    ?assertEqual(#globber{separator = <<".">>,
                          wildcard_one = <<"+">>,
                          wildcard_some = <<"#">>},
                 Globber2).

add(_Config) ->
    Globber = rabbit_mqtt_topic_matcher:new(),
    Globber1 = rabbit_mqtt_topic_matcher:add(Globber, <<"test/+">>, <<"matches">>),
    ?assertMatch(#globber{trie = _}, Globber1),
    Globber2 = rabbit_mqtt_topic_matcher:add(Globber1, <<"test/#">>, <<"it n">>),
    ?assertMatch(#globber{trie = _}, Globber2).

remove(_Config) ->
    Globber = rabbit_mqtt_topic_matcher:new(),
    Globber1 = rabbit_mqtt_topic_matcher:add(Globber, <<"test/+">>, <<"matches">>),
    Globber2 = rabbit_mqtt_topic_matcher:remove(Globber1, <<"test/+">>, <<"matches">>),
    ?assertEqual(Globber, Globber2).

match(_Config) ->
    Globber = rabbit_mqtt_topic_matcher:new(),
    Globber1 = rabbit_mqtt_topic_matcher:add(Globber, <<"test/+">>, <<"it matches">>),
    Result = rabbit_mqtt_topic_matcher:match(Globber1, <<"test/bar">>),
    ?assertEqual([<<"it matches">>], Result),
    Result2 = rabbit_mqtt_topic_matcher:match(Globber1, <<"test/foo">>),
    ?assertEqual([<<"it matches">>], Result2),
    Result3 = rabbit_mqtt_topic_matcher:match(Globber1, <<"not/foo">>),
    ?assertEqual([], Result3).

pattern_is_matching(_Config) ->
    Globber = rabbit_mqtt_topic_matcher:new(),
    Globber1 = rabbit_mqtt_topic_matcher:add(Globber, <<"test/+">>),
    ?assertEqual(true, rabbit_mqtt_topic_matcher:test(Globber1, <<"test/bar">>)),
    ?assertEqual(false, rabbit_mqtt_topic_matcher:test(Globber1, <<"foo/bar">>)).

match_iter(_Config) ->
    Globber = rabbit_mqtt_topic_matcher:new(),
    Globber1 = rabbit_mqtt_topic_matcher:add(Globber, <<"test/+">>, <<"matches">>),
    Result = rabbit_mqtt_topic_matcher:match_iter(Globber1, <<"test/bar">>),
    ?assertEqual([<<"matches">>], Result).

clear(_Config) ->
    Globber = rabbit_mqtt_topic_matcher:new(),
    Globber1 = rabbit_mqtt_topic_matcher:add(Globber, <<"test/+">>, <<"matches">>),
    Globber2 = rabbit_mqtt_topic_matcher:clear(Globber1),
    ?assertEqual(Globber, Globber2).

multiple_patterns(_Config) ->
    Globber = rabbit_mqtt_topic_matcher:new(<<".">>, <<"*">>, <<"#">>),
    Globber1 = rabbit_mqtt_topic_matcher:add(Globber, <<"foo.#">>, <<"catchall">>),
    Globber2 =
        rabbit_mqtt_topic_matcher:add(Globber1, <<"foo.*.bar">>, <<"single_wildcard">>),
    Globber3 =
        rabbit_mqtt_topic_matcher:add(Globber2, <<"foo.*.bar.#">>, <<"single_and_catchall">>),

    ?assertEqual([<<"catchall">>], rabbit_mqtt_topic_matcher:match(Globber3, <<"foo.bar">>)),
    ?assertEqual([<<"catchall">>],
                 rabbit_mqtt_topic_matcher:match(Globber3, <<"foo.bar.baz">>)),

    ?assertEqual([<<"catchall">>, <<"single_wildcard">>],
                 rabbit_mqtt_topic_matcher:match(Globber3, <<"foo.test.bar">>)),
    ?assertEqual([<<"catchall">>],
                 rabbit_mqtt_topic_matcher:match(Globber3, <<"foo.test.baz.bar">>)),

    ?assertEqual([<<"catchall">>, <<"single_and_catchall">>],
                 rabbit_mqtt_topic_matcher:match(Globber3, <<"foo.test.bar.baz">>)),
    ?assertEqual([<<"catchall">>, <<"single_and_catchall">>],
                 rabbit_mqtt_topic_matcher:match(Globber3, <<"foo.test.bar.baz.qux">>)).
