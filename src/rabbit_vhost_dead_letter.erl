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
-export([stop/1]).
-export([publish/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {queue_states,
                queue_cleanup_timer}).

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

stop(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            ok = supervisor2:terminate_child(VHostSup, rabbit_vhost_dead_letter),
            ok = supervisor2:delete_child(VHostSup, rabbit_vhost_dead_letter);
        {error, {no_such_vhost, VHost}} ->
            rabbit_log:error("Failed to stop a dead letter process for vhost ~s: "
                             "vhost no longer exists!", [VHost]),

            ok
    end.

publish(VHost, X, RK, QName, ReasonMsgs) ->
    case vhost_dead_letter_pid(VHost) of
        no_pid ->
            %% TODO what to do???
            ok;
        Pid ->
            gen_server:cast(Pid, {publish, X, RK, QName, ReasonMsgs})
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
    {ok, init_queue_cleanup_timer(#state{queue_states = #{}})}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({publish, X, RK, QName, ReasonMsgs}, #state{queue_states = QueueStates0} = State)
  when is_record(X, exchange) ->
    QueueStates = batch_publish(X, RK, QName, ReasonMsgs, QueueStates0),
    {noreply, State#state{queue_states = QueueStates}};
handle_cast({publish, DLX, RK, QName, ReasonMsgs}, #state{queue_states = QueueStates0} = State) ->
    QueueStates =
        case rabbit_exchange:lookup(DLX) of
            {ok, X} ->
                batch_publish(X, RK, QName, ReasonMsgs, QueueStates0);
            {error, not_found} ->
                QueueStates0
        end,
    {noreply, State#state{queue_states = QueueStates}}.

handle_info({ra_event, {Name, _}, _} = Evt,
            #state{queue_states = QueueStates} = State0) ->
    FState0 = maps:get(Name, QueueStates),
    case rabbit_quorum_queue:handle_event(Evt, FState0) of
        {_, _, _, FState1} ->
            {noreply,
             State0#state{queue_states = maps:put(Name, FState1, QueueStates)}};
        eol ->
            {noreply,
             State0#state{queue_states = maps:remove(Name, QueueStates)}}
    end;
handle_info(queue_cleanup, State = #state{queue_states = QueueStates0}) ->
    QueueStates = maps:filter(fun(Name, _) ->
                                      QName = rabbit_quorum_queue:queue_name(Name),
                                      case rabbit_amqqueue:lookup(QName) of
                                          [] ->
                                              false;
                                          _ ->
                                              true
                                      end
                              end, QueueStates0),
    {noreply, init_queue_cleanup_timer(State#state{queue_states = QueueStates})};
handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.

batch_publish(X, RK, QName, ReasonMsgs, QueueStates) ->
    lists:foldl(fun({Reason, Msg}, Acc) ->
                        rabbit_dead_letter:publish(Msg, Reason, X, RK, QName, Acc)
                end, QueueStates, ReasonMsgs).

init_queue_cleanup_timer(State) ->
    {ok, Interval} = application:get_env(rabbit, channel_queue_cleanup_interval),
    State#state{queue_cleanup_timer = erlang:send_after(Interval, self(), queue_cleanup)}.
