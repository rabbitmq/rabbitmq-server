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

-behaviour(gen_server2).

-export([start_link/0, notify_exclusive_queue/2, delete_all/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {exclusive_queues}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()}).
-spec(notify_exclusive_queue/2 :: (pid(), pid()) -> {'ok'}).
-spec(delete_all/1 :: (pid()) -> {'ok'}).

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link(?MODULE, [], []).

notify_exclusive_queue(CollectorPid, QPid) ->
    gen_server2:call(CollectorPid, {notify_exclusive_queue, QPid}, infinity).

delete_all(CollectorPid) ->
    gen_server2:call(CollectorPid, delete_all, infinity).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{exclusive_queues = sets:new()}}.

%%--------------------------------------------------------------------------

handle_call({notify_exclusive_queue, QPid}, _From,
            State = #state{exclusive_queues = Queues}) ->
    erlang:monitor(process, QPid),
    {reply, ok, State#state{exclusive_queues = sets:add_element(QPid, Queues)}};

handle_call(delete_all, _From,
            State = #state{exclusive_queues = ExclusiveQueues}) ->
    [rabbit_misc:with_exit_handler(
        fun() -> ok end,
        fun() -> gen_server2:call(QPid, {delete, false, false}, infinity) end)
        || QPid <- sets:to_list(ExclusiveQueues)],
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason},
            State = #state{exclusive_queues = ExclusiveQueues}) ->
    {noreply, State#state{exclusive_queues =
                          sets:del_element(DownPid, ExclusiveQueues)}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
