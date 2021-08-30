%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
    gen_server:call(CollectorPid, delete_all, infinity).

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
        _         -> ok = rabbit_amqqueue_common:delete_exclusive([QPid], Deleting)
    end,
    {reply, ok, State#state{monitors = pmon:monitor(QPid, QMons)}};

handle_call(delete_all, From, State = #state{monitors    = QMons,
                                             delete_from = undefined}) ->
    case pmon:monitored(QMons) of
        []    -> {reply, ok, State#state{delete_from = From}};
        QPids -> ok = rabbit_amqqueue_common:delete_exclusive(QPids, From),
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
