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
%% Copyright (c) 2011-2011 VMware, Inc.  All rights reserved.
%%

%% @doc A behaviour module for implementing consumers for amqp_channel. To
%% specify a consumer implementation for a channel, use
%% amqp_connection:open_channel/{2,3}.<br/>
%% All callbacks are called withing the channel process.<br/>
%% <br/>
%% See comments in amqp_gen_consumer.erl source file for documentation on the
%% callback functions.
-module(amqp_gen_consumer).

-export([behaviour_info/1]).

%%---------------------------------------------------------------------------
%% Behaviour
%%---------------------------------------------------------------------------

%% @private
behaviour_info(callbacks) ->
    [
     %% init(Args) -> InitialState
     %% where
     %%      Args = [any()]
     %%      InitialState = state()
     %%
     %% This callback is invoked by the channel, when it starts up. Use it to
     %% initialize the state of the consumer.
     {init, 1},

     %% handle_consume_ok(ConsumeOk, Consume, State) -> NewState
     %% where
     %%      ConsumeOk = #'basic.consume_ok'{}
     %%      Consume = #'basic.consume'{}
     %%      State = state()
     %%      NewState = state()
     %%
     %% This callback is invoked by the channel every time a
     %% basic.consume_ok is received from the server. Consume is the original
     %% method sent out to the server - it can be used to associate the
     %% call with the response.
     {handle_consume_ok, 3},

     %% handle_cancel_ok(CancelOk, Cancel, State) -> NewState
     %% where
     %%      CancelOk = #'basic.cancel_ok'{}
     %%      Cancel = #'basic.cancel'{}
     %%      State = state()
     %%      NewState = state()
     %%
     %% This callback is invoked by the channel every time a basic.cancel_ok
     %% is received from the server.
     {handle_cancel_ok, 3},

     %% handle_cancel(Cancel, State) -> NewState
     %% where
     %%      Cancel = #'basic.cancel'{}
     %%      State = state()
     %%      NewState = state()
     %%
     %% This callback is invoked by the channel every time a basic.cancel
     %% is received from the server.
     {handle_cancel, 2},

     %% handle_deliver(Deliver, State) -> NewState
     %% where
     %%      Deliver = {#'basic.deliver'{}, #amqp_msg{}}
     %%      State = state()
     %%      NewState = state()
     %%
     %% This callback is invoked by the channel every time a basic.deliver
     %% is received from the server.
     {handle_deliver, 2},

     %% handle_call(Call, State) -> {Reply, NewState}
     %% where
     %%      Call = any()
     %%      Reply = any()
     %%      State = state()
     %%      NewState = state()
     %%
     %% This callback is invoked by the channel when calling
     %% amqp_channel:call_consumer/2. Reply is the term that
     %% amqp_channel:call_consumer/2 will return.
     {handle_call, 2},

     %% terminate(Reason, State) -> NewState
     %% where
     %%      Reason = any()
     %%      State = state()
     %%      NewState = state()
     %%
     %% This callback is invoked by the channel after it has shut down and
     %% just before its process exits.
     {terminate, 2}
    ];
behaviour_info(_Other) ->
    undefined.
