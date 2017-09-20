%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_auth_cache_ets_segmented).
-behaviour(gen_server2).
-behaviour(rabbit_auth_cache).

-export([start_link/1,
         get/1, put/3, delete/1]).
-export([gc/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    segments = [],
    gc_timer,
    segment_size}).

start_link(SegmentSize) ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [SegmentSize], []).

get(Key) ->
    case get_from_segments(Key) of
        []    -> {error, not_found};
        [V|_] -> {ok, V}
    end.

put(Key, Value, TTL) ->
    Expiration = rabbit_auth_cache:expiration(TTL),
    Segment = gen_server2:call(?MODULE, {get_write_segment, Expiration}),
    ets:insert(Segment, {Key, {Expiration, Value}}),
    ok.

delete(Key) ->
    [ets:delete(Table, Key)
     || Table <- gen_server2:call(?MODULE, get_segment_tables)].

gc() ->
    case whereis(?MODULE) of
        undefined -> ok;
        Pid       -> Pid ! gc
    end.

init([SegmentSize]) ->
    InitSegment = ets:new(segment, [set, public]),
    InitBoundary = rabbit_auth_cache:expiration(SegmentSize),
    {ok, GCTimer} = timer:send_interval(SegmentSize * 2, gc),
    {ok, #state{gc_timer = GCTimer, segment_size = SegmentSize,
                segments = [{InitBoundary, InitSegment}]}}.

handle_call({get_write_segment, Expiration}, _From,
            State = #state{segments = Segments,
                           segment_size = SegmentSize}) ->
    [{_, Segment} | _] = NewSegments = maybe_add_segment(Expiration, SegmentSize, Segments),
    {reply, Segment, State#state{segments = NewSegments}};
handle_call(get_segment_tables, _From, State = #state{segments = Segments}) ->
    {_, Valid} = partition_expired_segments(Segments),
    {_,Tables} = lists:unzip(Valid),
    {reply, Tables, State}.

handle_cast(_, State = #state{}) ->
    {noreply, State}.

handle_info(gc, State = #state{ segments = Segments }) ->
    {Expired, Valid} = partition_expired_segments(Segments),
    [ets:delete(Table) || {_, Table} <- Expired],
    {noreply, State#state{ segments = Valid }};
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State = #state{gc_timer = Timer}) ->
    timer:cancel(Timer),
    State.

partition_expired_segments(Segments) ->
    lists:partition(
        fun({Boundary, _}) -> rabbit_auth_cache:expired(Boundary) end,
        Segments).

maybe_add_segment(Expiration, SegmentSize, OldSegments) ->
    case OldSegments of
        [{OldBoundary, _}|_] when OldBoundary > Expiration ->
            OldSegments;
        _ ->
            NewBoundary = Expiration + SegmentSize,
            Segment = ets:new(segment, [set, public]),
            [{NewBoundary, Segment} | OldSegments]
    end.

get_from_segments(Key) ->
    Tables = gen_server2:call(?MODULE, get_segment_tables),
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

