%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_osiris_metrics).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TICK_TIMEOUT, 5000).
-define(REPLICA_FRESHNESS_REFRESH_INTERVAL, 30000).
-define(SERVER, ?MODULE).

-define(STATISTICS_KEYS,
        [policy,
         operator_policy,
         effective_policy_definition,
         state,
         leader,
         online,
         members,
         memory,
         readers,
         consumers,
         segments,
         first_timestamp,
         replica_freshness_status
        ]).

-define(STATISTICS_KEYS_NO_REPLICA_FRESHNESS,
        ?STATISTICS_KEYS -- [replica_freshness_status]).

-record(state, {timeout :: non_neg_integer(),
               replica_freshness_cache = #{} :: #{rabbit_types:rabbit_amqqueue_name() =>
                                                 {integer(), term()}}}).

%%----------------------------------------------------------------------------
%% Starts the raw metrics storage and owns the ETS tables.
%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    Timeout = application:get_env(rabbit, stream_tick_interval,
                                  ?TICK_TIMEOUT),
    erlang:send_after(Timeout, self(), tick),
    {ok, #state{timeout = Timeout}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(tick, #state{timeout = Timeout,
                         replica_freshness_cache = Cache0} = State) ->
    Data = osiris_counters:overview(),
    Now = erlang:monotonic_time(millisecond),
    {Cache1, ActiveQNamesRev} = maps:fold(
      fun ({osiris_writer, QName}, #{offset := Offs,
                                     first_offset := FstOffs}, {Cache, ActiveQNames}) ->
              COffs = Offs + 1 - FstOffs,
              rabbit_core_metrics:queue_stats(QName, COffs, 0, COffs, 0),
              {Infos, Cache1} = try
                                    %% TODO complete stats!
                                    case rabbit_amqqueue:lookup(QName) of
                                        {ok, Q} ->
                                            stream_infos(QName, Q, Now, Cache);
                                        _ ->
                                            {[], Cache}
                                    end
                                catch
                                    _:_ ->
                                        %% It's possible that the writer has died but
                                        %% it's still on the amqqueue record, so the
                                        %% `erlang:process_info/2` calls will return
                                        %% `undefined` and crash with a badmatch.
                                        %% At least for now, skipping the metrics might
                                        %% be the best option. Otherwise this brings
                                        %% down `rabbit_sup` and the whole `rabbit` app.
                                        {[], Cache}
                                end,
              rabbit_core_metrics:queue_stats(QName, Infos),
              {Cache1, [QName | ActiveQNames]};
          (_, _V, Acc) ->
              Acc
      end, {Cache0, []}, Data),
    Cache2 = maps:with(ActiveQNamesRev, Cache1),
    erlang:send_after(Timeout, self(), tick),
    {noreply, State#state{replica_freshness_cache = Cache2}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

stream_infos(QName, Q, Now, Cache0) ->
    Infos0 = rabbit_stream_queue:info(Q, ?STATISTICS_KEYS_NO_REPLICA_FRESHNESS),
    {ReplicaFreshness, Cache1} = replica_freshness_status(QName, Q, Now, Cache0),
    {Infos0 ++ [{replica_freshness_status, ReplicaFreshness}], Cache1}.

replica_freshness_status(QName, Q, Now, Cache0) ->
    case maps:get(QName, Cache0, undefined) of
        {RefreshedAt, ReplicaFreshness}
          when Now - RefreshedAt < ?REPLICA_FRESHNESS_REFRESH_INTERVAL ->
            {ReplicaFreshness, Cache0};
        _ ->
            refresh_replica_freshness_status(QName, Q, Now, Cache0)
    end.

refresh_replica_freshness_status(QName, Q, Now, Cache0) ->
    try rabbit_stream_queue:info(Q, [replica_freshness_status]) of
        [{replica_freshness_status, ReplicaFreshness}] ->
            {ReplicaFreshness, Cache0#{QName => {Now, ReplicaFreshness}}};
        _ ->
            stale_or_unknown_replica_freshness(QName, Cache0)
    catch
        _:_ ->
            stale_or_unknown_replica_freshness(QName, Cache0)
    end.

stale_or_unknown_replica_freshness(QName, Cache0) ->
    case maps:get(QName, Cache0, undefined) of
        {_, ReplicaFreshness} ->
            {ReplicaFreshness, Cache0};
        undefined ->
            {#{}, Cache0}
    end.
