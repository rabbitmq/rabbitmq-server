-module(mqtt_machine_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("mqtt_machine.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     basics
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

basics(_Config) ->
    S0 = mqtt_machine:init(#{}),
    ClientId = <<"id1">>,
    {S1, ok, _} = mqtt_machine:apply(meta(1), {register, ClientId, self()}, S0),
    ?assertMatch(#machine_state{client_ids = Ids} when map_size(Ids) == 1, S1),
    {S2, ok, _} = mqtt_machine:apply(meta(2), {register, ClientId, self()}, S1),
    ?assertMatch(#machine_state{client_ids = Ids} when map_size(Ids) == 1, S2),
    {S3, ok, _} = mqtt_machine:apply(meta(3), {down, self(), noproc}, S2),
    ?assertMatch(#machine_state{client_ids = Ids} when map_size(Ids) == 0, S3),
    {S4, ok, _} = mqtt_machine:apply(meta(3), {unregister, ClientId, self()}, S2),
    ?assertMatch(#machine_state{client_ids = Ids} when map_size(Ids) == 0, S4),

    ok.

%% Utility

meta(Idx) ->
    #{index => Idx,
      term => 1,
      ts => erlang:system_time(millisecond)}.
