%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_prequeue).

%% This is the initial gen_server that all queue processes start off
%% as. It handles the decision as to whether we need to start a new
%% slave, a new master/unmirrored, whether we lost a race to declare a
%% new queue, or whether we are in recovery. Thus a crashing queue
%% process can restart from here and always do the right thing.

-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-behaviour(gen_server2).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([start_mode/0]).

-type(start_mode() :: 'declare' | 'recovery' | 'slave').

-spec(start_link/3 :: (rabbit_types:amqqueue(), start_mode(), pid())
                      -> rabbit_types:ok_pid_or_error()).

-endif.

%%----------------------------------------------------------------------------

start_link(Q, StartMode, Marker) ->
    gen_server2:start_link(?MODULE, {Q, StartMode, Marker}, []).

%%----------------------------------------------------------------------------

init({Q, StartMode0, Marker}) ->
    %% Hand back to supervisor ASAP
    gen_server2:cast(self(), init),
    StartMode = case is_process_alive(Marker) of
                    true  -> StartMode0;
                    false -> restart
                end,
    {ok, {Q#amqqueue{pid = self()}, StartMode}, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
      ?DESIRED_HIBERNATE}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, {unexpected_call, Msg}, State}.

handle_cast(init, {Q, declare})  -> init_declared(Q);
handle_cast(init, {Q, recovery}) -> init_recovery(Q);
handle_cast(init, {Q, slave})    -> init_slave(Q);
handle_cast(init, {Q, restart})  -> init_restart(Q);

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

init_declared(Q = #amqqueue{name = QueueName}) ->
    Decl = rabbit_misc:execute_mnesia_transaction(
             fun () ->
                     case mnesia:wread({rabbit_queue, QueueName}) of
                         []          -> rabbit_amqqueue:internal_declare(Q);
                         [ExistingQ] -> {existing, ExistingQ}
                     end
             end),
    %% We have just been declared. Block waiting for an init
    %% call so that we don't respond to any other message first
    receive {'$gen_call', From, {init, new}} ->
            case Decl of
                {new, Fun} ->
                    Q1 = Fun(),
                    rabbit_amqqueue_process:init_declared(new,From, Q1);
                {F, _} when F =:= absent; F =:= existing ->
                    gen_server2:reply(From, Decl),
                    {stop, normal, Q}
            end
    end.

init_recovery(Q) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> ok = rabbit_amqqueue:store_queue(Q) end),
    %% Again block waiting for an init call.
    receive {'$gen_call', From, {init, Terms}} ->
            rabbit_amqqueue_process:init_declared(Terms, From, Q)
    end.

init_slave(Q) ->
    rabbit_mirror_queue_slave:init_slave(Q).

init_restart(#amqqueue{name = QueueName}) ->
    {ok, Q = #amqqueue{pid        = QPid,
                       slave_pids = SPids}} = rabbit_amqqueue:lookup(QueueName),
    Local = node(QPid) =:= node(),
    Slaves = [SPid || SPid <- SPids, rabbit_misc:is_process_alive(SPid)],
    case rabbit_misc:is_process_alive(QPid) of
        true  -> false = Local, %% assertion
                 rabbit_mirror_queue_slave:init_slave(Q); %% [1]
        false -> case Local andalso Slaves =:= [] of
                     true  -> crash_restart(Q);           %% [2]
                     false -> timer:sleep(25),
                              init_restart(Q)             %% [3]
                 end
    end.
%% [1] There is a master on another node. Regardless of whether we
%%     were originally a master or a slave, we are now a new slave.
%%
%% [2] Nothing is alive. We are the last best hope. Try to restart as a master.
%%
%% [3] The current master is dead but either there are alive slaves to
%%     take over or it's all happening on a different node anyway. This is
%%     not a stable situation. Sleep and wait for somebody else to make a
%%     move.

crash_restart(Q = #amqqueue{name = QueueName}) ->
    rabbit_log:error("Restarting crashed ~s.~n", [rabbit_misc:rs(QueueName)]),
    Self = self(),
    rabbit_misc:execute_mnesia_transaction(
      fun () -> ok = rabbit_amqqueue:store_queue(Q#amqqueue{pid = Self}) end),
    rabbit_amqqueue_process:init_declared(
      {no_barrier, non_clean_shutdown}, none, Q).
