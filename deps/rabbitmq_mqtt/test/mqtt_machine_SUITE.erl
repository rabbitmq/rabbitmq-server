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
     basics,
     machine_upgrade,
     many_downs
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
    OthPid = spawn(fun () -> ok end),
    {S1, ok, _} = mqtt_machine:apply(meta(1), {register, ClientId, self()}, S0),
    ?assertMatch(#machine_state{client_ids = Ids} when map_size(Ids) == 1, S1),
    ?assertMatch(#machine_state{pids = Pids} when map_size(Pids) == 1, S1),
    {S2, ok, _} = mqtt_machine:apply(meta(2), {register, ClientId, OthPid}, S1),
    ?assertMatch(#machine_state{client_ids = #{ClientId := OthPid} = Ids}
                   when map_size(Ids) == 1, S2),
    {S3, ok, _} = mqtt_machine:apply(meta(3), {down, OthPid, noproc}, S2),
    ?assertMatch(#machine_state{client_ids = Ids} when map_size(Ids) == 0, S3),
    {S4, ok, _} = mqtt_machine:apply(meta(3), {unregister, ClientId, OthPid}, S2),
    ?assertMatch(#machine_state{client_ids = Ids} when map_size(Ids) == 0, S4),

    ok.

machine_upgrade(_Config) ->
    S0 = mqtt_machine_v0:init(#{}),
    ClientId = <<"id1">>,
    Self = self(),
    {S1, ok, _} = mqtt_machine_v0:apply(meta(1), {register, ClientId, self()}, S0),
    ?assertMatch({machine_state, Ids} when map_size(Ids) == 1, S1),
    {S2, ok, _} = mqtt_machine:apply(meta(2), {machine_version, 0, 1}, S1),
    ?assertMatch(#machine_state{client_ids = #{ClientId := Self},
                                pids = #{Self := [ClientId]} = Pids}
                                  when map_size(Pids) == 1, S2),
    {S3, ok, _} = mqtt_machine:apply(meta(3), {down, self(), noproc}, S2),
    ?assertMatch(#machine_state{client_ids = Ids,
                                pids = Pids}
                   when map_size(Ids) == 0 andalso map_size(Pids) == 0, S3),

    ok.

many_downs(_Config) ->
    S0 = mqtt_machine:init(#{}),
    Clients = [{list_to_binary(integer_to_list(I)), spawn(fun() -> ok end)}
               || I <- lists:seq(1, 10000)],
    S1 = lists:foldl(
           fun ({ClientId, Pid}, Acc0) ->
                   {Acc, ok, _} = mqtt_machine:apply(meta(1), {register, ClientId, Pid}, Acc0),
                   Acc
           end, S0, Clients),
    _ = lists:foldl(
           fun ({_ClientId, Pid}, Acc0) ->
                   {Acc, ok, _} = mqtt_machine:apply(meta(1), {down, Pid, noproc}, Acc0),
                   Acc
           end, S1, Clients),
    _ = lists:foldl(
           fun ({ClientId, Pid}, Acc0) ->
                   {Acc, ok, _} = mqtt_machine:apply(meta(1), {unregister, ClientId,
                                                               Pid}, Acc0),
                   Acc
           end, S0, Clients),

    ok.
%% Utility

meta(Idx) ->
    #{index => Idx,
      term => 1,
      ts => erlang:system_time(millisecond)}.
