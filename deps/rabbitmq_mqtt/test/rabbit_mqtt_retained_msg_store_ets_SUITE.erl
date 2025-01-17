-module(rabbit_mqtt_retained_msg_store_ets_SUITE).

-compile([export_all, nowarn_export_all]).

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
     test_combined_wildcards]}].

init_per_suite(Config) ->
  Config.

end_per_suite(_Config) ->
  ok.

init_per_group(_Group, Config) ->
  Config.

end_per_group(_Group, _Config) ->
  ok.

init_per_testcase(_TestCase, Config) ->
  % Create a unique directory for each test case
  Dir = filename:join(["/tmp", "mqtt_test_" ++ integer_to_list(erlang:unique_integer())]),
  ok = filelib:ensure_dir(Dir ++ "/"),
  State = rabbit_mqtt_retained_msg_store_ets:new(Dir, <<"test">>),
  [{store_state, State}, {test_dir, Dir} | Config].

end_per_testcase(_TestCase, Config) ->
  State = ?config(store_state, Config),
  Dir = ?config(test_dir, Config),
  rabbit_mqtt_retained_msg_store_ets:terminate(State),
  % Clean up test directory
  os:cmd("rm -rf " ++ Dir),
  ok.

%%----------------------------------------------------------------------------
%% Test Cases
%%----------------------------------------------------------------------------

test_add_and_match(Config) ->
  State = ?config(store_state, Config),

  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  Matches1 = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/c">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/d">>, <<"msg2">>, State),
  Matches2 = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/d">>, State),
  NoMatches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"x/y/z">>, State),

  ?_assertEqual([<<"msg1">>], Matches1),
  ?_assertEqual([<<"msg2">>], Matches2),
  ?_assertEqual([], NoMatches).

test_delete(Config) ->
  State = ?config(store_state, Config),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:delete(<<"a/b/c">>, State),
  Matches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/c">>, State),

  ?_assertEqual([], Matches).

test_plus_wildcard(Config) ->
  State = ?config(store_state, Config),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/x/c">>, <<"msg2">>, State),
  Matches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/+/c">>, State),

  ?_assertEqual(lists:sort([<<"msg1">>, <<"msg2">>]), lists:sort(Matches)).

test_hash_wildcard(Config) ->
  State = ?config(store_state, Config),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c/d">>, <<"msg2">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/x/y">>, <<"msg3">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/q/x/y">>, <<"msg3">>, State),
  Matches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/b/#">>, State),

  ?_assertEqual([<<"msg1">>, <<"msg2">>, <<"msg3">>], lists:sort(Matches)).

test_combined_wildcards(Config) ->
  State = ?config(store_state, Config),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/b/c/d">>, <<"msg1">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/x/c/e">>, <<"msg2">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/y/c/f/g">>, <<"msg3">>, State),
  ok = rabbit_mqtt_retained_msg_store_ets:insert(<<"a/y/d/f/g">>, <<"msg4">>, State),
  Matches = rabbit_mqtt_retained_msg_store_ets:lookup(<<"a/+/c/#">>, State),

  ?_assertEqual([<<"msg1">>, <<"msg2">>, <<"msg3">>], lists:sort(Matches)).
