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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_queue_collector).

%% Queue collector keeps track of exclusive queues and cleans them
%% up e.g. when their connection is closed.

-behaviour(gen_server).

-export([start_link/1, register/2, delete_all/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {monitors, delete_from}).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(rabbit_types:proc_name()) -> rabbit_types:ok_pid_or_error().

start_link(ProcName) ->
    gen_server:start_link(?MODULE, [ProcName], []).

-spec register(pid(), pid()) -> 'ok'.

register(CollectorPid, Q) ->
    gen_server:call(CollectorPid, {register, Q}, infinity).

delete_all(CollectorPid) ->
    rabbit_queue_collector_common:delete_all(CollectorPid).

%%----------------------------------------------------------------------------

init([ProcName]) ->
    ?LG_PROCESS_TYPE(queue_collector),
    ?store_proc_name(ProcName),
    {ok, #state{monitors = pmon:new(), delete_from = undefined}}.

%%--------------------------------------------------------------------------

handle_call({register, QPid}, _From,
            State = #state{monitors = QMons, delete_from = Deleting}) ->
    case Deleting of
        undefined -> ok;
        _         -> ok = rabbit_amqqueue:delete_exclusive([QPid], Deleting)
    end,
    {reply, ok, State#state{monitors = pmon:monitor(QPid, QMons)}};

handle_call(delete_all, From, State = #state{monitors    = QMons,
                                             delete_from = undefined}) ->
    case pmon:monitored(QMons) of
        []    -> {reply, ok, State#state{delete_from = From}};
        QPids -> ok = rabbit_amqqueue:delete_exclusive(QPids, From),
                 {noreply, State#state{delete_from = From}}
    end.

handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info({'DOWN', _MRef, process, DownPid, _Reason},
            State = #state{monitors = QMons, delete_from = Deleting}) ->
    QMons1 = pmon:erase(DownPid, QMons),
    case Deleting =/= undefined andalso pmon:is_empty(QMons1) of
        true  -> gen_server:reply(Deleting, ok);
        false -> ok
    end,
    {noreply, State#state{monitors = QMons1}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
