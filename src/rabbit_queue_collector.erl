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

-module(rabbit_queue_collector).

-behaviour(gen_server).

-export([start_link/0, register/2, delete_all/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queues}).

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
    {ok, #state{queues = dict:new()}}.

%%--------------------------------------------------------------------------

handle_call({register, Q}, _From,
            State = #state{queues = Queues}) ->
    MonitorRef = erlang:monitor(process, Q#amqqueue.pid),
    {reply, ok,
     State#state{queues = dict:store(MonitorRef, Q, Queues)}};

handle_call(delete_all, _From, State = #state{queues = Queues}) ->
    Qs = dict:to_list(Queues),
    [erlang:demonitor(MonitorRef) || {MonitorRef, _} <- Qs],
    scatter(fun({_, Q}) ->
                    rabbit_misc:with_exit_handler(
                      fun () -> ok end,
                      fun () -> rabbit_amqqueue:delete_exclusive(Q) end)
            end, delete_done, Qs),
    gather(delete_done, dict:size(Queues)),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info({'DOWN', MonitorRef, process, _DownPid, _Reason},
            State = #state{queues = Queues}) ->
    {noreply, State#state{queues = dict:erase(MonitorRef, Queues)}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------------

%% scatter(Fun, Tag, Xs): run Fun(X) for each X in Xs concurrently;
%% when a job finishes, send back Tag
scatter(Fun, Tag, Xs) ->
    Self = self(),
    [spawn(fun() -> Fun(X),
                    Self ! Tag
           end) || X <- Xs].

%% gather(MsgAtom, N): gather N messages like MsgAtom
gather(_MsgAtom, 0) -> ok;
gather(MsgAtom, N)  -> receive
                           MsgAtom -> gather(MsgAtom, N-1)
                       end.
