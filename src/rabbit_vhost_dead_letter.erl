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
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_vhost_dead_letter).

-include("rabbit.hrl").

-behaviour(gen_server).

-export([start/1, start_link/0]).
-export([publish/6]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {queue_states}).

start(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            supervisor2:start_child(VHostSup,
                                    {rabbit_vhost_dead_letter,
                                     {rabbit_vhost_dead_letter, start_link, []},
                                     transient, ?WORKER_WAIT, worker, [rabbit_vhost_dead_letter]});
        {error, {no_such_vhost, VHost}} = E ->
            rabbit_log:error("Failed to start a dead letter process for vhost ~s: vhost no"
                             " longer exists!", [VHost]),
            E
    end.

publish(VHost, Msg, Reason, X, RK, QName) ->
    case vhost_dead_letter_pid(VHost) of
        no_pid ->
            %% TODO what to do???
            ok;
        Pid ->
            gen_server:cast(Pid, {publish, Msg, Reason, X, RK, QName})
    end.

vhost_dead_letter_pid(VHost) ->
    {ok, VHostSup} = rabbit_vhost_sup_sup:get_vhost_sup(VHost),
    case supervisor2:find_child(VHostSup, rabbit_vhost_dead_letter) of
        [Pid] -> Pid;
        []    -> no_pid
    end.

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #state{queue_states = #{}}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({publish, Msg, Reason, X, RK, QName}, #state{queue_states = QueueStates0} = State) ->
    QueueStates = rabbit_dead_letter:publish(Msg, Reason, X, RK, QName, QueueStates0),
    {noreply, State#state{queue_states = QueueStates}}.

handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.
