%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% USAGE
%% run one of the `rabbit_chaos:begin_*` functions on as many nodes as you want, for example:
%% for i in 1 2 3; do rabbitmqctl eval 'rabbit_chaos:begin_quorum_chaos().'; done
%%
%% rabbit_chaos:begin/0 unleashes the chaos in all subsystems with the interval of 20s
%%
%% you can use `rabbit_chaos:stop()` to stop injecting failures.

-module(rabbit_chaos).
-behaviour(gen_server).


-rabbit_boot_step({rabbit_chaos,
                   [{description, "rabbit node chaos server"},
                    {mfa, {rabbit_sup, start_restartable_child,
                           [rabbit_chaos]}},
                    {requires, [database]},
                    {enables, core_initialized}]}).

-export([start_link/0]).
-export([
         begin_chaos/0,
         begin_chaos/1,
         begin_quorum_chaos/0,
         begin_quorum_chaos/1,
         begin_coordination_chaos/0,
         begin_coordination_chaos/1,
         begin_delayed_chaos/0,
         begin_delayed_chaos/1,
         begin_jms_chaos/0,
         begin_jms_chaos/1,
         stop/0
        ]).


-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type chaos_event() :: {Name :: atom(), Weight :: non_neg_integer(),
                        {kill_named_proc, Process :: atom(), Reason :: atom()} |
                        {kill_ra_member, Reason :: atom()} |
                        restart_ra_member |
                        flood_node |
                        {multi, non_neg_integer(),
                         non_neg_integer(), [chaos_event()]}
                       }.

-type chaos_cfg() :: #{ra_systems := [atom()],
                       interval := non_neg_integer(),
                       events := [chaos_event()]}.
-define(SERVER, ?MODULE).

-record(?MODULE, {cfg :: chaos_cfg(),
                  timer_ref :: undefined | reference()}).

-export_type([chaos_cfg/0,
              chaos_event/0]).

-include_lib("kernel/include/logger.hrl").

%%----------------------------------------------------------------------------
%% A chaos server that can be enabled to create periodic configurable chaos
%% inside the broker.
%%----------------------------------------------------------------------------

-define(ALL_SYSTEMS, [quorum_queues, coordination, delayed_queues, jms_queues]).
-define(DEFAULT_INTERVAL, 20000).

begin_chaos() ->
    begin_chaos(?DEFAULT_INTERVAL).

begin_chaos(Interval) when is_integer(Interval) ->
    begin_for_systems(?ALL_SYSTEMS, Interval);
begin_chaos(Cfg) when is_map(Cfg) ->
    gen_server:call(?SERVER, {begin_chaos, Cfg}).

begin_quorum_chaos() ->
    begin_quorum_chaos(?DEFAULT_INTERVAL).

begin_quorum_chaos(Interval) ->
    begin_for_systems([quorum_queues], Interval).

begin_coordination_chaos() ->
    begin_coordination_chaos(?DEFAULT_INTERVAL).

begin_coordination_chaos(Interval) ->
    begin_for_systems([coordination], Interval).

begin_delayed_chaos() ->
    begin_delayed_chaos(?DEFAULT_INTERVAL).

begin_delayed_chaos(Interval) ->
    begin_for_systems([delayed_queues], Interval).

begin_jms_chaos() ->
    begin_jms_chaos(?DEFAULT_INTERVAL).

begin_jms_chaos(Interval) ->
    begin_for_systems([jms_queues], Interval).

stop() ->
    gen_server:call(?SERVER, stop_chaos).

-spec start_link() -> rabbit_types:ok_pid_or_error().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    Cfg = #{interval => ?DEFAULT_INTERVAL,
            ra_systems => [],
            events => []},
    {ok, #?MODULE{cfg = Cfg, timer_ref = undefined}}.

handle_call({begin_chaos, #{interval := Interval} = Cfg}, _From, State0) ->
    State = cancel_timer(State0),
    Ref = erlang:send_after(Interval, self(), do_chaos),
    {reply, ok, State#?MODULE{cfg = Cfg, timer_ref = Ref}};
handle_call(stop_chaos, _From, State0) ->
    State = cancel_timer(State0),
    Cfg = #{interval => ?DEFAULT_INTERVAL,
            ra_systems => [],
            events => []},
    ?LOG_INFO("~s: chaos stopped", [?MODULE]),
    {reply, ok, State#?MODULE{cfg = Cfg}}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(do_chaos, #?MODULE{cfg = #{ra_systems := Systems,
                                       interval := Interval} = Cfg} = State) ->
    Events = maps:get(events, Cfg),
    {Name, _, Event} = pick_event(Events),
    Sys = pick_random(Systems),
    do_event(Sys, Name, Event),
    Ref = erlang:send_after(Interval, self(), do_chaos),
    {noreply, State#?MODULE{timer_ref = Ref}};
handle_info(_, #?MODULE{} = State) ->
    {noreply, State}.

terminate(_Reason, #?MODULE{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal

begin_for_systems(Systems, Interval) ->
    Events = lists:flatmap(fun events_for_system/1, Systems)
             ++ common_events(),
    begin_chaos(#{ra_systems => Systems,
                  interval => Interval,
                  events => Events}).

events_for_system(quorum_queues) ->
    [{kill_qq_wal, 1, {kill_named_proc, ra_log_wal, chaos}},
     {kill_qq_seg_writer, 1, {kill_named_proc, ra_log_segment_writer, kill}},
     {kill_qq_member, 2, {kill_ra_member, chaos}},
     {restart_qq_member, 2, restart_ra_member}];
events_for_system(coordination) ->
    [{kill_coord_wal, 1, {kill_named_proc, ra_coordination_log_wal, chaos}},
     {kill_coord_seg_writer, 1,
      {kill_named_proc, ra_coordination_segment_writer, kill}},
     {kill_coord_member, 2, {kill_ra_member, chaos}},
     {restart_coord_member, 2, restart_ra_member}];
events_for_system(delayed_queues) ->
    [{kill_dq_wal, 1, {kill_named_proc, ra_delayed_queues_log_wal, chaos}},
     {kill_dq_seg_writer, 1,
      {kill_named_proc, ra_delayed_queues_segment_writer, kill}},
     {kill_dq_member, 2, {kill_ra_member, chaos}},
     {restart_dq_member, 2, restart_ra_member}];
events_for_system(jms_queues) ->
    [{kill_jms_wal, 1, {kill_named_proc, ra_jms_queues_log_wal, chaos}},
     {kill_jms_seg_writer, 1,
      {kill_named_proc, ra_jms_queues_segment_writer, kill}},
     {kill_jms_member, 2, {kill_ra_member, chaos}},
     {restart_jms_member, 2, restart_ra_member}].

common_events() ->
    [{flood_a_node, 2, flood_node}].

cancel_timer(#?MODULE{timer_ref = undefined} = State) ->
    State;
cancel_timer(#?MODULE{timer_ref = Ref} = State) ->
    _ = erlang:cancel_timer(Ref),
    State#?MODULE{timer_ref = undefined}.

pick_random(List) ->
    lists:nth(rand:uniform(length(List)), List).

do_event(_Sys, Name, {kill_named_proc, ProcName, ExitReason}) ->
    ?LOG_INFO("~s: doing event ~s...", [?MODULE, Name]),
    catch exit(whereis(ProcName), ExitReason),
    ok;
do_event(_Sys, Name, flood_node) ->
    Nodes = nodes(),
    case Nodes of
        [] -> ok;
        _ ->
            ?LOG_INFO("~s: doing event ~s...", [?MODULE, Name]),
            At = rand:uniform(length(Nodes)),
            Selected = lists:nth(At, Nodes),

            Pid = erpc:call(Selected, erlang, spawn, [fun() -> ok end]),

            Data = crypto:strong_rand_bytes(100_000),
            Loop = fun F(0) -> ok;
                       F(N) ->
                           case erlang:send(Pid, Data, [nosuspend]) of
                               nosuspend ->
                                   Pid ! Data,
                                   ?LOG_INFO("~s: flood of node ~s competed ~s...",
                                             [?MODULE, Selected, Name]),
                                   ok;
                               ok ->
                                   F(N-1)
                           end
                   end,

            Loop(10000),
            ok
    end;
do_event(Sys, Name, {kill_ra_member, ExitReason}) ->
    ?LOG_INFO("~s: doing event ~s in Ra system ~s...", [?MODULE, Name, Sys]),
    case list_registered_safe(Sys) of
        [] ->
            ?LOG_INFO("~s: no Ra members in system ~s, skipping", [?MODULE, Sys]),
            ok;
        Registered ->
            {Selected, _UId} = pick_random(Registered),
            {ok, _, _} = ra:local_query({Selected, node()},
                                        fun (_) -> process_flag(trap_exit, false) end),
            catch exit(whereis(Selected), ExitReason),
            ok
    end;
do_event(Sys, Name, restart_ra_member = Type) ->
    ?LOG_INFO("~s: doing event ~s of type ~s in Ra system ~s",
              [?MODULE, Name, Type, Sys]),
    case list_registered_safe(Sys) of
        [] ->
            ?LOG_INFO("~s: no Ra members in system ~s, skipping", [?MODULE, Sys]),
            ok;
        Registered ->
            {ServerName, _UId} = pick_random(Registered),
            ServerId = {ServerName, node()},
            _ = ra:stop_server(Sys, ServerId),
            Sleep = rand:uniform(10000) + 1000,
            timer:sleep(Sleep),
            _ = ra:restart_server(Sys, ServerId),
            ok
    end;
do_event(Sys, Name, {multi, Num, Interval, Event}) ->
    ?LOG_INFO("~s: doing multi event ~s...",
              [?MODULE, Name]),
    catch [begin
               do_event(Sys, Name, Event),
               timer:sleep(Interval)
           end || _ <- lists:seq(1, Num)],
    ok.

list_registered_safe(Sys) ->
    try
        ra_directory:list_registered(Sys)
    catch
        _:_ -> []
    end.

pick_event(Events) ->
    TotalWeight = lists:sum([element(2, E) || E <- Events]),
    Pick = rand:uniform(TotalWeight),
    event_at_weight_point(Pick, 0, Events).


event_at_weight_point(_Pick, _Cur, []) ->
    undefined;
event_at_weight_point(Pick, Cur0, [{_, W, _} = E | Events]) ->
    Cur = Cur0 + W,
    case Pick =< Cur of
        true ->
           E;
        false ->
            event_at_weight_point(Pick, Cur, Events)
    end.
