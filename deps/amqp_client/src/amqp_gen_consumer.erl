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

%% @doc A behaviour module for implementing consumers for amqp_channel. To
%% specify a consumer implementation for a channel, use
%% amqp_connection:open_channel/{2,3}.<br/>
%% All callbacks are called withing the channel process.
-module(amqp_gen_consumer).

-export([behaviour_info/1]).

%%---------------------------------------------------------------------------
%% Behaviour
%%---------------------------------------------------------------------------

%% all callbacks run in the channel process

%% @private
behaviour_info(callbacks) ->
    [
     %% @spec Module:init(Args) -> {ok, InitialState}
     %% where
     %%      Args = [any()]
     %%      InitialState = any()
     %% @doc This function is called by the channel, when it starts up.
     {init, 1},

     %% @type consume_ok() = #'basic.consume_ok'{}.
     %% The AMQP method returned in response to basic.consume.
     %% @spec Module:handle_consume(consume_ok(), State) -> {ok, NewState}
     %% where
     %%      State = NewState = any()
     %% @doc This function is called by the channel every time a
     %% basic.consume_ok is received from the server.
     {handle_consume_ok, 2},

     %% @type cancel_ok() = #'basic.cancel_ok'{}.
     %% The AMQP method returned as reply to basicl.cancel.
     %% @spec Module:handle_cancel_ok(cancel_ok(), State) -> {ok, NewState}
     %% where
     %%      State = NewState = any()
     %% @doc This function is called by the channel every time a basic.cancel_ok
     %% is received from the server.
     {handle_cancel_ok, 2},

     %% @type cancel() = #'basic.cancel'{}.
     %% The AMQP method used to cancel a subscription.
     %% @spec Module:handle_cancel(cancel(), State) -> {ok, NewState}
     %% where
     %%      State = NewState = any()
     %% @doc This function is called by the channel every time a basic.cancel
     %% is received from the server.
     {handle_cancel, 2},

     %% @type deliver() = #'basic.deliver'{}.
     %% The AMQP method sent when a message is delivered from a subscribed
     %% queue.
     %% @spec Module:handle_deliver(deliver(), #amqp_msg{}, State} ->
     %%           {ok, NewState}
     %% where
     %%      State = NewState = any()
     %% @doc This function is called by the channel every time a basic.deliver
     %% is received from the server.
     {handle_deliver, 3},

     %% @spec Module:handle_message(Message, State) -> {ok, NewState}
     %% where
     %%      State = NewState = any()
     %% @doc This function is called by the channel when calling
     %% amqp_channel:send_to_consumer/2.
     {handle_message, 2},

     %% @spec Module:terminate(Reason, State) -> _
     %% where
     %%      State = any()
     %%      Reason = any()
     %% @doc This function is called by the channel after it has shut down and
     %% just before its process exits.
     {terminate, 2}
    ];
behaviour_info(_Other) ->
    undefined.
