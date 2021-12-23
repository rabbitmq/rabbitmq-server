%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_osiris_metrics).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TICK_TIMEOUT, 5000).
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
         readers
        ]).

-record(state, {timeout :: non_neg_integer()}).

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

handle_info(tick, #state{timeout = Timeout} = State) ->
    Data = osiris_counters:overview(),
    maps:map(
      fun ({osiris_writer, QName}, #{offset := Offs,
                                     first_offset := FstOffs}) ->
              COffs = Offs + 1 - FstOffs,
              rabbit_core_metrics:queue_stats(QName, COffs, 0, COffs, 0),
              Infos = try
                          %% TODO complete stats!
                          case rabbit_amqqueue:lookup(QName) of
                              {ok, Q} ->
                                  rabbit_stream_queue:info(Q, ?STATISTICS_KEYS);
                              _ ->
                                  []
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
                              []
                      end,


              rabbit_core_metrics:queue_stats(QName, Infos),
              rabbit_event:notify(queue_stats, Infos ++ [{name, QName},
                                                         {messages, COffs},
                                                         {messages_ready, COffs},
                                                         {messages_unacknowledged, 0}]),
              ok;
          (_, _V) ->
              ok
      end, Data),
    erlang:send_after(Timeout, self(), tick),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
