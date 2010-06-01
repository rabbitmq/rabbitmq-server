%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_reader_queue_collector).

-behaviour(gen_server).

-export([start_link/0, register_exclusive_queue/2, delete_all/1, shutdown/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {exclusive_queues}).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()}).
-spec(register_exclusive_queue/2 :: (pid(), amqqueue()) -> 'ok').
-spec(delete_all/1 :: (pid()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

register_exclusive_queue(CollectorPid, Q) ->
    gen_server:call(CollectorPid, {register_exclusive_queue, Q}, infinity).

delete_all(CollectorPid) ->
    gen_server:call(CollectorPid, delete_all, infinity).

shutdown(CollectorPid) ->
    gen_server:call(CollectorPid, shutdown, infinity).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{exclusive_queues = dict:new()}}.

%%--------------------------------------------------------------------------

handle_call({register_exclusive_queue, Q}, _From,
            State = #state{exclusive_queues = Queues}) ->
    MonitorRef = erlang:monitor(process, Q#amqqueue.pid),
    {reply, ok,
     State#state{exclusive_queues = dict:store(MonitorRef, Q, Queues)}};

handle_call(delete_all, _From,
            State = #state{exclusive_queues = ExclusiveQueues}) ->
    [rabbit_misc:with_exit_handler(
       fun () -> ok end,
       fun () ->
               erlang:demonitor(MonitorRef),
               rabbit_amqqueue:delete(Q, false, false)
       end)
     || {MonitorRef, Q} <- dict:to_list(ExclusiveQueues)],
    {reply, ok, State};

handle_call(shutdown, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, _DownPid, _Reason},
            State = #state{exclusive_queues = ExclusiveQueues}) ->
    {noreply, State#state{exclusive_queues =
                              dict:erase(MonitorRef, ExclusiveQueues)}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
