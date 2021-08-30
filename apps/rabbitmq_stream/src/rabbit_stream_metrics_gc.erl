%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_metrics_gc).

-behaviour(gen_server).

-include_lib("rabbitmq_stream/include/rabbit_stream_metrics.hrl").

-record(state, {timer, interval}).

-export([start_link/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-spec start_link() -> rabbit_types:ok_pid_or_error().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    Interval =
        rabbit_misc:get_env(rabbit, core_metrics_gc_interval, 120000),
    {ok, start_timer(#state{interval = Interval})}.

handle_call(which_children, _From, State) ->
    {reply, [], State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(start_gc, State) ->
    GbSet =
        gb_sets:from_list(
            rabbit_amqqueue:list_names()),
    gc_process_and_entity(?TABLE_CONSUMER, GbSet),
    gc_process_and_entity(?TABLE_PUBLISHER, GbSet),
    {noreply, start_timer(State)}.

terminate(_Reason, #state{timer = TRef}) ->
    erlang:cancel_timer(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_timer(#state{interval = Interval} = St) ->
    TRef = erlang:send_after(Interval, self(), start_gc),
    St#state{timer = TRef}.

gc_process_and_entity(Table, GbSet) ->
    ets:foldl(fun({{Id, Pid, _} = Key, _}, none) ->
                 gc_process_and_entity(Id, Pid, Table, Key, GbSet)
              end,
              none, Table).

gc_process_and_entity(Id, Pid, Table, Key, GbSet) ->
    case rabbit_misc:is_process_alive(Pid)
         andalso gb_sets:is_member(Id, GbSet)
    of
        true ->
            none;
        false ->
            ets:delete(Table, Key),
            none
    end.
