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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

%% @doc TODO
-module(amqp_gen_consumer).

-export([behaviour_info/1]).

%%---------------------------------------------------------------------------
%% Behaviour
%%---------------------------------------------------------------------------

%% TODO: doc

%% all callbacks run in the channel process

%% init: called when channel is started.
%% handle_consume_ok: called on each basic.consume_ok
%% handle_cancel_ok: called on each basic.cancel_ok
%% handle_deliver: called on each basic.deliver
%% handle_info: called on amqp_channel:send_to_consumer/2
%% terminate: called after channel has shut down

behaviour_info(callbacks) ->
    [
     %% init(Args) -> {ok, InitialState}
     {init, 1},

     %% handle_consume(#'basic.consume_ok'{}, State) -> {ok, NewState}
     {handle_consume_ok, 2},

     %% handle_cancel(#'basic.cancel_ok'{}, State) -> {ok, NewState}
     {handle_cancel_ok, 2},
     
     %% handle_deliver(#'basic.deliver', #amqp_msg{}, State} -> {ok, NewState}
     {handle_deliver, 3},

     %% handle_info(Message, State) -> {ok, NewState}
     {handle_info, 2},

     %% terminate(Reason, State) -> _
     {terminate, 2}
    ];
behaviour_info(_Other) ->
    undefined.
