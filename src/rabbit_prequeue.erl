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

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-behaviour(gen_server2).

-include("rabbit.hrl").

start_link(Q) ->
    gen_server2:start_link(?MODULE, Q, []).

%%----------------------------------------------------------------------------

init(Q) ->
    %% Hand back to supervisor ASAP
    gen_server2:cast(self(), init),
    {ok, Q#amqqueue{pid = self()}, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
      ?DESIRED_HIBERNATE}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(init, Q) ->
    case whereis(rabbit_recovery) of
        undefined -> init_non_recovery(Q);
        _Pid      -> init_recovery(Q)
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

init_non_recovery(Q = #amqqueue{name = QueueName}) ->
    Result = rabbit_misc:execute_mnesia_transaction(
               fun () ->
                       case mnesia:wread({rabbit_queue, QueueName}) of
                           [] ->
                               {decl, rabbit_amqqueue:internal_declare(Q)};
                           [ExistingQ = #amqqueue{pid = QPid}] ->
                               case rabbit_misc:is_process_alive(QPid) of
                                   true  -> {decl, {existing, ExistingQ}};
                                   false -> exit(todo)
                               end
                       end
               end),
    case Result of
        {decl, DeclResult} ->
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
            end
    end.

init_recovery(Q) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> ok = rabbit_amqqueue:store_queue(Q) end),
    %% Again block waiting for an init call.
    receive {'$gen_call', From, {init, Terms}} ->
            rabbit_amqqueue_process:init_declared(Terms, From, Q)
    end.
