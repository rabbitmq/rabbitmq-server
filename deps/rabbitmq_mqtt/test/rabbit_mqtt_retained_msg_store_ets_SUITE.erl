%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mqtt_retained_msg_store_ets_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("rabbitmq_mqtt/include/rabbit_mqtt_packet.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
  [{group, tests}].

groups() ->
  [{tests,
    [parallel],
    [test_add_and_match,
     test_delete,
     test_plus_wildcard,
     test_hash_wildcard,
     test_combined_wildcards,
     test_recovery]}].

init_per_suite(Config) ->
  Config.

end_per_suite(_Config) ->
  ok.

init_per_group(_Group, Config) ->
  Config.

end_per_group(_Group, _Config) ->
  ok.

init_per_testcase(_TestCase, Config) ->
  Dir = filename:join(["/tmp", "mqtt_test_" ++ integer_to_list(erlang:unique_integer())]),
  ok = filelib:ensure_dir(Dir ++ "/"),
  State = rabbit_mqtt_retained_msg_store_ets:new(Dir, <<"test">>),
  [{store_state, State}, {test_dir, Dir} | Config].

end_per_testcase(_TestCase, Config) ->
  State = ?config(store_state, Config),
  Dir = ?config(test_dir, Config),
  rabbit_mqtt_retained_msg_store_ets:terminate(State),
  os:cmd("rm -rf " ++ Dir),
  ok.

%%----------------------------------------------------------------------------
%% Test Cases
%%----------------------------------------------------------------------------

test_add_and_match(Config) ->
  State = ?config(store_state, Config),
  Msg1 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/c">>,
              dup = false,
              payload = <<"msg1">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg2 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/d">>,
              dup = false,
              payload = <<"msg2">>,
              props = #{},
              timestamp = os:system_time(second)},
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, Msg1, State),
  Matches1 = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/c">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/d">>, Msg2, State),
  Matches2 = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/d">>, State),
  NoMatches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"x/y/z">>, State),

  ?assertEqual([Msg1], Matches1),
  ?assertEqual([Msg2], Matches2),
  ?assertEqual([], NoMatches).

test_delete(Config) ->
  State = ?config(store_state, Config),
  Msg1 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/c">>,
              dup = false,
              payload = <<"msg1">>,
              props = #{},
              timestamp = os:system_time(second)},
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, Msg1, State),
  ok = rabbit_mqtt_retained_msg_store_ets:delete(<<"a/b/c">>, State),
  Matches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/c">>, State),

  ?assertEqual([], Matches).

test_plus_wildcard(Config) ->
  State = ?config(store_state, Config),
  Msg1 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/c">>,
              dup = false,
              payload = <<"msg1">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg2 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/x/c">>,
              dup = false,
              payload = <<"msg2">>,
              props = #{},
              timestamp = os:system_time(second)},
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, Msg1, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/x/c">>, Msg2, State),
  Matches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/+/c">>, State),

  ?assertEqual(lists:sort([Msg1, Msg2]), lists:sort(Matches)).

test_hash_wildcard(Config) ->
  State = ?config(store_state, Config),
  Msg1 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/c">>,
              dup = false,
              payload = <<"msg1">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg2 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/c/d">>,
              dup = false,
              payload = <<"msg2">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg3 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/x/y">>,
              dup = false,
              payload = <<"msg3">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg4 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/q/x/y">>,
              dup = false,
              payload = <<"msg4">>,
              props = #{},
              timestamp = os:system_time(second)},
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, Msg1, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c/d">>, Msg2, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/x/y">>, Msg3, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/q/x/y">>, Msg4, State),
  Matches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/#">>, State),

  ?assertEqual([Msg1, Msg2, Msg3], lists:sort(Matches)).

test_combined_wildcards(Config) ->
  State = ?config(store_state, Config),
  Msg1 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/c">>,
              dup = false,
              payload = <<"msg1">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg2 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/d">>,
              dup = false,
              payload = <<"msg2">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg3 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/x/c/e">>,
              dup = false,
              payload = <<"msg3">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg4 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/y/c/f/g">>,
              dup = false,
              payload = <<"msg4">>,
              props = #{},
              timestamp = os:system_time(second)},
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c/d">>, Msg1, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/x/c/e">>, Msg2, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/y/c/f/g">>, Msg3, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/y/d/f/g">>, Msg4, State),
  Matches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/+/c/#">>, State),

  ?assertEqual([Msg1, Msg2, Msg3], lists:sort(Matches)).

test_recovery(Config) ->
  State = ?config(store_state, Config),
  Msg1 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/c">>,
              dup = false,
              payload = <<"msg1">>,
              props = #{},
              timestamp = os:system_time(second)},
  Msg2 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/d">>,
              dup = false,
              payload = <<"msg2">>,
              props = #{},
              timestamp = os:system_time(second)},

  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, Msg1, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/d">>, Msg2, State),
  Matches1 = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/c">>, State),
  Matches2 = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/d">>, State),
  NoMatches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"x/y/z">>, State),
  ?assertEqual([Msg1], Matches1),
  ?assertEqual([Msg2], Matches2),
  ?assertEqual([], NoMatches),
  % Recover the state
  ok = rabbit_mqtt_retained_msg_store_ets:terminate(State),
  {ok, Filenames} = file:list_dir(?config(test_dir, Config)),
  ?assertEqual(3, length(Filenames)),

  {ok, State2, _Expire} =
    rabbit_mqtt_retained_msg_store_ets:recover(?config(test_dir, Config), <<"test">>),

  Matches1 = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/c">>, State2),
  Matches2 = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/d">>, State2),
  NoMatches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"x/y/z">>, State2),
  ?assertEqual([Msg1], Matches1),
  ?assertEqual([Msg2], Matches2),
  ?assertEqual([], NoMatches).
