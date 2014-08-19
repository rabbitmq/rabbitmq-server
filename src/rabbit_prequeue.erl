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

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-behaviour(gen_server2).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

%%-spec(start_link/2 :: () -> rabbit_types:ok_pid_or_error()).

-endif.

%%----------------------------------------------------------------------------

start_link(Q, Hint) ->
    gen_server2:start_link(?MODULE, {Q, Hint}, []).

%%----------------------------------------------------------------------------

init({Q, Hint}) ->
    %% Hand back to supervisor ASAP
    gen_server2:cast(self(), init),
    {ok, {Q#amqqueue{pid = self()}, Hint}, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
      ?DESIRED_HIBERNATE}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, {unexpected_call, Msg}, State}.

handle_cast(init, {Q, Hint}) ->
    case whereis(rabbit_recovery) of
        undefined -> init_non_recovery(Q, Hint);
        _Pid      -> recovery = Hint, %% assertion
                     init_recovery(Q)
    end;

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

init_non_recovery(Q = #amqqueue{name = QueueName}, Hint) ->
    Result = rabbit_misc:execute_mnesia_transaction(
               fun () ->
                       case mnesia:wread({rabbit_queue, QueueName}) of
                           []          -> init_missing(Q, Hint);
                           [ExistingQ] -> init_existing(ExistingQ)
                       end
               end),
    case Result of
        {declared, DeclResult} ->
            %% We have just been declared. Block waiting for an init
            %% call so that we don't respond to any other message first
            receive {'$gen_call', From, {init, new}} ->
                    case DeclResult of
                        {new, Fun} ->
                            Q1 = Fun(),
                            rabbit_amqqueue_process:init_declared(new,From, Q1);
                        {F, _} when F =:= absent; F =:= existing ->
                            gen_server2:reply(From, DeclResult),
                            {stop, normal, Q}
                    end
            end;
        new_slave ->
            rabbit_mirror_queue_slave:init_slave(Q);
        {crash_restart, Q1} ->
            rabbit_log:error(
              "Recovering persistent messages from crashed ~s.~n",
              [rabbit_misc:rs(QueueName)]),
            Self = self(),
            rabbit_misc:execute_mnesia_transaction(
              fun () ->
                      ok = rabbit_amqqueue:store_queue(Q1#amqqueue{pid = Self})
              end),
            rabbit_amqqueue_process:init_declared(
              {no_barrier, non_clean_shutdown}, none, Q1);
        sleep_retry ->
            timer:sleep(25),
            init_non_recovery(Q, Hint);
        master_in_recovery ->
            {stop, normal, Q}
    end.

%% The Hint is how we were originally started. Of course, if we
%% crashed it might no longer be true - but we can only get here if
%% there is no Mnesia record, which should mean we can't be here if we
%% crashed.
init_missing(Q, Hint) ->
    case Hint of
        declare -> {declared, rabbit_amqqueue:internal_declare(Q)};
        slave   -> master_in_recovery %% [1]
    end.
%% [1] This is the same concept as the master_in_recovery case in the
%%     slave startup code. Unfortunately since we start slaves with two
%%     transactions we need to check twice.

init_existing(Q = #amqqueue{pid = QPid, slave_pids = SPids}) ->
    Alive = fun rabbit_misc:is_process_alive/1,
    case {Alive(QPid), node(QPid) =:= node()} of
        {true,  true}  -> {declared, {existing, Q}};     %% [1]
        {true,  false} -> new_slave;                     %% [2]
        {false, _}     -> case [SPid || SPid <- SPids, Alive(SPid)] of
                              [] -> {crash_restart, Q};  %% [3]
                              _  -> sleep_retry          %% [4]
                          end
    end.
%% [1] Lost a race to declare a queue - just return the winner.
%%
%% [2] There is a master on another node. Regardless of whether we
%%     just crashed (as a master or slave) and restarted or were asked to
%%     start as a slave, we are now a new slave.
%%
%% [3] Nothing is alive. We must have just died. Try to restart as a master.
%%
%% [4] The current master is dead but there are alive slaves. This is
%%     not a stable situation. Sleep and wait for somebody else to make a
%%     move - those slaves should either promote one of their own or die.

init_recovery(Q) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> ok = rabbit_amqqueue:store_queue(Q) end),
    %% Again block waiting for an init call.
    receive {'$gen_call', From, {init, Terms}} ->
            rabbit_amqqueue_process:init_declared(Terms, From, Q)
    end.
