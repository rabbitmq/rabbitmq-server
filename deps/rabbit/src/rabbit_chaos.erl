%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_chaos).

-behaviour(gen_server).

-export([start_link/0]).
-export([
         begin_default/0,
         begin_default/1,
         begin_chaos/1
        ]).


-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type chaos_event() :: {Name :: atom(),
                        {kill_named_proc, Process :: atom()} |
                        kill_quorum_queue_member}.

-type chaos_cfg() :: #{interval := non_neg_integer(),
                       events := [chaos_event()]}.
-define(SERVER, ?MODULE).

-record(?MODULE, {cfg :: chaos_cfg()}).

-export_type([chaos_cfg/0,
              chaos_event/0]).

%%----------------------------------------------------------------------------
%% A chaos server that can be enabled to create periodic configurable chaos
%% inside the broker.
%%----------------------------------------------------------------------------

begin_default() ->
    begin_default(20000).

begin_default(Interval) ->
    Events = [
              {kill_qq_wal, 5, {kill_named_proc, ra_log_wal}},
              {kill_qq_seq_writer, 1, {kill_named_proc, ra_log_segment_writer}},
              {kill_qq_member, 1, kill_ra_member},
              {kill_qq_member, 1, restart_ra_member},
              {multi_kill_qq_wal, 1,
               {multi, 3, 100, {kill_named_proc, ra_log_wal}}}
             ],
    begin_chaos(#{interval => Interval,
                  events => Events}).

begin_chaos(Cfg) ->
    gen_server:call(?SERVER, {begin_chaos, Cfg}).

-spec start_link() -> rabbit_types:ok_pid_or_error().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    Cfg = #{interval => 20000,
            events => []},
    {ok, #?MODULE{cfg = Cfg}}.

handle_call({begin_chaos, #{interval := Interval} = Cfg}, _From, State) ->
    _ = erlang:send_after(Interval, self(), do_chaos),
    {reply, ok, State#?MODULE{cfg = Cfg}}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(do_chaos, #?MODULE{cfg = #{interval := Interval} = Cfg} = State) ->
    Events = maps:get(events, Cfg),
    {Name, _, Event} = pick_event(Events),
    do_event(Name, Event),
    _ = erlang:send_after(Interval, self(), do_chaos),
    {noreply, State};
handle_info(_, #?MODULE{} = State) ->
    {noreply, State}.

terminate(_Reason, #?MODULE{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal

do_event(Name, {kill_named_proc, ProcName}) ->
    rabbit_log:info("~s: doing event ~s...", [?MODULE, Name]),
    catch exit(whereis(ProcName), chaos),
    ok;
do_event(Name, kill_ra_member) ->
    rabbit_log:info("~s: doing event ~s...", [?MODULE, Name]),
    Procs = ets:tab2list(ra_leaderboard),
    At = rand:uniform(length(Procs)),
    Selected = lists:nth(At, Procs),
    catch exit(whereis(element(1, Selected)), kill),
    ok;
do_event(Name, restart_ra_member = Type) ->
    rabbit_log:info("~s: doing event ~s of type ~s", [?MODULE, Name, Type]),
    Queues = rabbit_amqqueue:list_local_quorum_queues(),
    At = rand:uniform(length(Queues)),
    Selected = lists:nth(At, Queues),
    ServerId = amqqueue:get_pid(Selected),
    ra:stop_server(quorum_queues, ServerId),
    ra:restart_server(quorum_queues, ServerId),
    ok;
do_event(Name, {multi, Num, Interval, Event}) ->
    rabbit_log:info("~s: doing multi event ~s...",
                    [?MODULE, Name]),
    catch [begin
               do_event(Name, Event),
               timer:sleep(Interval)
           end || _ <- lists:seq(1, Num)],
    ok.

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
