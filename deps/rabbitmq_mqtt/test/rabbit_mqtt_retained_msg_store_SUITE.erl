%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mqtt_retained_msg_store_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("rabbitmq_mqtt/include/rabbit_mqtt_packet.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
  [{group, ets}, {group, dets}].

groups() ->
  [{dets, [parallel], tests()}, {ets, [parallel], tests()}].

tests() ->
  [test_add_and_match,
   test_delete,
   test_plus_wildcard,
   test_hash_wildcard,
   test_combined_wildcards,
   test_recovery].

init_per_suite(Config) ->
  Config.

end_per_suite(_Config) ->
  ok.

init_per_group(G, Config) ->
  case G of
    ets ->
      Module = rabbit_mqtt_retained_msg_store_ets;
    dets ->
      Module = rabbit_mqtt_retained_msg_store_dets
  end,
  [{module, Module} | Config].

end_per_group(_Group, _Config) ->
  ok.

init_per_testcase(_TestCase, Config) ->
  Mod = ?config(module, Config),

  Dir = filename:join(["/tmp", "mqtt_test_" ++ integer_to_list(erlang:unique_integer())]),
  ok = filelib:ensure_dir(Dir ++ "/"),
  State = Mod:new(Dir, <<"test">>),
  [{store_state, State}, {test_dir, Dir} | Config].

end_per_testcase(_TestCase, Config) ->
  State = ?config(store_state, Config),
  Mod = ?config(module, Config),
  Dir = ?config(test_dir, Config),
  Mod:terminate(State),

  timer:sleep(100),
  case file:del_dir_r(Dir) of
    ok ->
      ok;
    {error, enoent} ->
      ok; % Directory already gone
    {error, Reason} ->
      ct:pal("Failed to delete directory ~p: ~p", [Dir, Reason]),
      % Try a more aggressive cleanup
      os:cmd("rm -rf " ++ Dir)
  end,
  ok.

%%----------------------------------------------------------------------------
%% Test Cases
%%----------------------------------------------------------------------------

test_add_and_match(Config) ->
  State = ?config(store_state, Config),
  Mod = ?config(module, Config),

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
  ok = Mod:insert(<<"a/b/c">>, Msg1, State),
  Matches1 = Mod:lookup(<<"a/b/c">>, State),
  ok = Mod:insert(<<"a/b/d">>, Msg2, State),
  Matches2 = Mod:lookup(<<"a/b/d">>, State),
  NoMatches = Mod:lookup(<<"x/y/z">>, State),

  ?assertEqual([Msg1], Matches1),
  ?assertEqual([Msg2], Matches2),
  ?assertEqual([], NoMatches).

test_delete(Config) ->
  State = ?config(store_state, Config),
  Mod = ?config(module, Config),
  Msg1 =
    #mqtt_msg{retain = true,
              qos = 0,
              topic = <<"a/b/c">>,
              dup = false,
              payload = <<"msg1">>,
              props = #{},
              timestamp = os:system_time(second)},
  ok = Mod:insert(<<"a/b/c">>, Msg1, State),
  ok = Mod:delete(<<"a/b/c">>, State),
  Matches = Mod:lookup(<<"a/b/c">>, State),

  ?assertEqual([], Matches).

test_plus_wildcard(Config) ->
  State = ?config(store_state, Config),
  Mod = ?config(module, Config),
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
  ok = Mod:insert(<<"a/b/c">>, Msg1, State),
  ok = Mod:insert(<<"a/x/c">>, Msg2, State),
  Matches = Mod:lookup(<<"a/+/c">>, State),

  ?assertEqual(lists:sort([Msg1, Msg2]), lists:sort(Matches)).

test_hash_wildcard(Config) ->
  State = ?config(store_state, Config),
  Mod = ?config(module, Config),
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
  ok = Mod:insert(<<"a/b/c">>, Msg1, State),
  ok = Mod:insert(<<"a/b/c/d">>, Msg2, State),
  ok = Mod:insert(<<"a/b/x/y">>, Msg3, State),
  ok = Mod:insert(<<"a/q/x/y">>, Msg4, State),
  Matches = Mod:lookup(<<"a/b/#">>, State),

  ?assertEqual([Msg1, Msg2, Msg3], lists:sort(Matches)).

test_combined_wildcards(Config) ->
  State = ?config(store_state, Config),
  Mod = ?config(module, Config),

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
  ok = Mod:insert(<<"a/b/c/d">>, Msg1, State),
  ok = Mod:insert(<<"a/x/c/e">>, Msg2, State),
  ok = Mod:insert(<<"a/y/c/f/g">>, Msg3, State),
  ok = Mod:insert(<<"a/y/d/f/g">>, Msg4, State),
  Matches = Mod:lookup(<<"a/+/c/#">>, State),

  ?assertEqual([Msg1, Msg2, Msg3], lists:sort(Matches)).

test_recovery(Config) ->
  State = ?config(store_state, Config),
  Mod = ?config(module, Config),

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

  ok = Mod:insert(<<"a/b/c">>, Msg1, State),
  ok = Mod:insert(<<"a/b/d">>, Msg2, State),
  Matches1 = Mod:lookup(<<"a/b/c">>, State),
  Matches2 = Mod:lookup(<<"a/b/d">>, State),
  NoMatches = Mod:lookup(<<"x/y/z">>, State),
  ?assertEqual([Msg1], Matches1),
  ?assertEqual([Msg2], Matches2),
  ?assertEqual([], NoMatches),
  % Recover the state
  ok = Mod:terminate(State),
  {ok, _Filenames} = file:list_dir(?config(test_dir, Config)),

  {ok, State2, _Expire} = Mod:recover(?config(test_dir, Config), <<"test">>),

  Matches1 = Mod:lookup(<<"a/b/c">>, State2),
  Matches2 = Mod:lookup(<<"a/b/d">>, State2),
  NoMatches = Mod:lookup(<<"x/y/z">>, State2),
  ?assertEqual([Msg1], Matches1),
  ?assertEqual([Msg2], Matches2),
  ?assertEqual([], NoMatches).
