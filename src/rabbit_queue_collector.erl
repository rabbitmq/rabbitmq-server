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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_queue_collector).

-behaviour(gen_server).

-export([start_link/0, register/2, delete_all/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queues, delete_from}).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(register/2 :: (pid(), rabbit_types:amqqueue()) -> 'ok').
-spec(delete_all/1 :: (pid()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

register(CollectorPid, Q) ->
    gen_server:call(CollectorPid, {register, Q}, infinity).

delete_all(CollectorPid) ->
    gen_server:call(CollectorPid, delete_all, infinity).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{queues = dict:new(), delete_from = undefined}}.

%%--------------------------------------------------------------------------

handle_call({register, Q}, _From,
            State = #state{queues = Queues, delete_from = Deleting}) ->
    MonitorRef = erlang:monitor(process, Q#amqqueue.pid),
    case Deleting of
        undefined -> ok;
        _         -> rabbit_amqqueue:delete_immediately(Q)
    end,
    {reply, ok, State#state{queues = dict:store(MonitorRef, Q, Queues)}};

handle_call(delete_all, From, State = #state{queues      = Queues,
                                             delete_from = undefined}) ->
    case dict:size(Queues) of
        0 -> {reply, ok, State#state{delete_from = From}};
        _ -> [rabbit_amqqueue:delete_immediately(Q)
              || {_MRef, Q} <- dict:to_list(Queues)],
             {noreply, State#state{delete_from = From}}
    end.

handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info({'DOWN', MonitorRef, process, _DownPid, _Reason},
            State = #state{queues = Queues, delete_from = Deleting}) ->
    Queues1 = dict:erase(MonitorRef, Queues),
    case Deleting =/= undefined andalso dict:size(Queues1) =:= 0 of
        true  -> gen_server:reply(Deleting, ok);
        false -> ok
    end,
    {noreply, State#state{queues = Queues1}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
