%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_cache_ets_segmented_stateless).
-behaviour(gen_server).
-behaviour(rabbit_auth_cache).

-include("rabbit_auth_backend_cache.hrl").

-export([start_link/1,
         get/1, put/3, delete/1]).
-export([gc/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SEGMENT_TABLE, rabbit_auth_cache_ets_segmented_stateless_segment_table).

-record(state, {gc_timer}).

start_link(SegmentSize) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [SegmentSize], []).

get(Key) ->
    case get_from_segments(Key) of
        []    -> {error, not_found};
        [V|_] -> {ok, V}
    end.

put(Key, Value, TTL) ->
    Expiration = rabbit_auth_cache:expiration(TTL),
    [{_, SegmentSize}] = ets:lookup(?SEGMENT_TABLE, segment_size),
    Segment = segment(Expiration, SegmentSize),
    Table = case ets:lookup(?SEGMENT_TABLE, Segment) of
        [{Segment, T}] -> T;
        []             -> add_segment(Segment)
    end,
    ets:insert(Table, {Key, {Expiration, Value}}),
    ok.

delete(Key) ->
    [ets:delete(Table, Key)
     || Table <- get_all_segment_tables()].

gc() ->
    case whereis(?MODULE) of
        undefined -> ok;
        Pid       -> Pid ! gc
    end.

init([SegmentSize]) ->
    ets:new(?SEGMENT_TABLE, [ordered_set, named_table, public]),
    ets:insert(?SEGMENT_TABLE, {segment_size, SegmentSize}),

    InitSegment = segment(rabbit_auth_cache:expiration(SegmentSize), SegmentSize),
    do_add_segment(InitSegment),

    {ok, GCTimer} = timer:send_interval(SegmentSize * 2, gc),
    {ok, #state{gc_timer = GCTimer}}.

handle_call({add_segment, Segment}, _From, State) ->
    %% Double check segment if it's already created
    Table = do_add_segment(Segment),
    {reply, Table, State}.

handle_cast(_, State = #state{}) ->
    {noreply, State}.

handle_info(gc, State = #state{}) ->
    Now = erlang:system_time(milli_seconds),
    MatchSpec = [{{'$1', '$2'}, [{'<', '$1', {const, Now}}], ['$2']}],
    Expired = ets:select(?SEGMENT_TABLE, MatchSpec),
    [ets:delete(Table) || Table <- Expired],
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State = #state{gc_timer = Timer}) ->
    timer:cancel(Timer),
    State.

segment(Expiration, SegmentSize) ->
    Begin = ((Expiration div SegmentSize) * SegmentSize),
    End = Begin + SegmentSize,
    End.

add_segment(Segment) ->
    gen_server:call(?MODULE, {add_segment, Segment}, ?CACHE_OPERATION_TIMEOUT).

do_add_segment(Segment) ->
    case ets:lookup(?SEGMENT_TABLE, Segment) of
        [{Segment, Table}] -> Table;
        []                 -> Table = ets:new(segment, [set, public]),
                              ets:insert(?SEGMENT_TABLE, {Segment, Table}),
                              Table
    end.

get_segment_tables() ->
    Now = erlang:system_time(milli_seconds),
    MatchSpec = [{{'$1', '$2'}, [{'>', '$1', {const, Now}}], ['$_']}],
    [V || {K, V} <- ets:select(?SEGMENT_TABLE, MatchSpec), K =/= segment_size].

get_all_segment_tables() ->
    [V || {K, V} <- ets:tab2list(?SEGMENT_TABLE), K =/= segment_size].

get_from_segments(Key) ->
    Tables = get_segment_tables(),
    lists:flatmap(
        fun(undefined) -> [];
           (T) ->
                try ets:lookup(T, Key) of
                    [{Key, {Exp, Val}}] ->
                        case rabbit_auth_cache:expired(Exp) of
                            true  -> [];
                            false -> [Val]
                        end;
                    [] -> []
                % ETS table can be deleted concurrently.
                catch
                    error:badarg -> []
                end
        end,
        Tables).

