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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_auth_cache_ets_segmented).
-behaviour(gen_server2).

-export([start_link/0,
         get/1, put/2, delete/1]).
-export([gc/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {boundary, old_segment, current_segment, garbage, gc_timer}).

start_link() -> gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) ->
    case get_from_segments(Key) of
        []    -> {error, not_found};
        [V|_] -> {ok, V}
    end.

put(Key, Value) ->
    TTL = cache_ttl(),
    Expiration = expiration(TTL),
    Segment = gen_server2:call(?MODULE, {get_write_segment, Expiration}),
    ets:insert(Segment, {Key, {Expiration, Value}}),
    ok.

delete(Key) ->
    [ets:delete(Table, Key) || Table <- gen_server2:call(?MODULE, get_segments)].

gc() ->
    case whereis(?MODULE) of
        undefined -> ok;
        Pid       -> Pid ! gc
    end.

init(_Args) ->
    Segment = ets:new(segment, [set, protected]),
    SegmentSize = segment_size(),
    Boundary = expiration(SegmentSize),
    {ok, Timer} = timer:send_interval(SegmentSize * 2, gc),
    {ok, #state{boundary = Boundary, current_segment = Segment, garbage = [], gc_timer = Timer}}.

handle_call({get_write_segment, Expiration}, _From,
            State = #state{boundary = Boundary,
                           current_segment = CurrentSegment,
                           old_segment = OldSegment,
                           garbage = Garbage}) when Boundary =< Expiration ->
    NewSegment = ets:new(segment, [set, protected]),
    NewBoundary = Boundary + segment_size(),
    {reply, NewSegment, State#state{boundary = NewBoundary,
                                    current_segment = NewSegment,
                                    old_segment = CurrentSegment,
                                    garbage = [OldSegment | Garbage]}};
handle_call({get_write_segment, _}, _From, State = #state{current_segment = CurrentSegment}) ->
    {reply, CurrentSegment, State};
handle_call(get_segments, _From, State = #state{old_segment = Old, current_segment = Current}) ->
    {reply, [Current, Old], State}.

handle_cast(_, State = #state{}) ->
    {noreply, State}.

handle_info(gc, State = #state{ garbage = Garbage }) ->
    [ets:delete(Segment) || Segment <- Garbage, Segment =/= undefined],
    {noreply, State#state{ garbage = [] }};
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State = #state{gc_timer = Timer}) ->
    erlang:cancel_timer(Timer),
    State.

segment_size() ->
    cache_ttl() * 2.

cache_ttl() ->
    application:get_env(rabbitmq_auth_backend_cache, cache_ttl).

expiration(TTL) ->
    time_compat:erlang_system_time(milliseconds) + TTL.

expired(Exp) ->
    time_compat:erlang_system_time(milliseconds) > Exp.

get_from_segments(Key) ->
    Segments = gen_server2:call(?MODULE, get_segments),
    lists:flatmap(
        fun(T) ->
            case ets:lookup(T, Key) of
                [{Key, {Exp, Val}}] ->
                    case expired(Exp) of
                        true  -> [];
                        false -> [Val]
                    end;
                 [] -> []
            end
        end,
        Segments).

