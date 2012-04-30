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
%% Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
%%

%% @doc A behaviour module for implementing consumers for
%% amqp_channel. To specify a consumer implementation for a channel,
%% use amqp_connection:open_channel/{2,3}.
%% <br/>
%% All callbacks are called within the gen_consumer process. <br/>
%% <br/>
%% See comments in amqp_gen_consumer.erl source file for documentation
%% on the callback functions.
%% <br/>
%% Note that making calls to the channel from the callback module will
%% result in deadlock.
-module(amqp_gen_consumer).

-include("amqp_client.hrl").

-behaviour(gen_server2).

-export([start_link/2, call_consumer/2, call_consumer/3]).
-export([behaviour_info/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, prioritise_info/2]).

-record(state, {module,
                module_state}).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

%% @type ok_error() = {ok, state()} | {error, reason(), state()}.
%% Denotes a successful or an error return from a consumer module call.

start_link(ConsumerModule, ExtraParams) ->
    gen_server2:start_link(?MODULE, [ConsumerModule, ExtraParams], []).

%% @spec (Consumer, Msg) -> ok
%% where
%%      Consumer = pid()
%%      Msg = any()
%%
%% @doc This function is used to perform arbitrary calls into the
%% consumer module.
call_consumer(Pid, Msg) ->
    gen_server2:call(Pid, {consumer_call, Msg}, infinity).

%% @spec (Consumer, Method, Args) -> ok
%% where
%%      Consumer = pid()
%%      Method = amqp_method()
%%      Args = any()
%%
%% @doc This function is used by amqp_channel to forward received
%% methods and deliveries to the consumer module.
call_consumer(Pid, Method, Args) ->
    gen_server2:call(Pid, {consumer_call, Method, Args}, infinity).

%%---------------------------------------------------------------------------
%% Behaviour
%%---------------------------------------------------------------------------

%% @private
behaviour_info(callbacks) ->
    [
     %% init(Args) -> {ok, InitialState} | {stop, Reason} | ignore
     %% where
     %%      Args = [any()]
     %%      InitialState = state()
     %%      Reason = term()
     %%
     %% This callback is invoked by the channel, when it starts
     %% up. Use it to initialize the state of the consumer. In case of
     %% an error, return {stop, Reason} or ignore.
     {init, 1},

     %% handle_consume(Consume, Sender, State) -> ok_error()
     %% where
     %%      Consume = #'basic.consume'{}
     %%      Sender = pid()
     %%      State = state()
     %%
     %% This callback is invoked by the channel before a basic.consume
     %% is sent to the server.
     {handle_consume, 3},

     %% handle_consume_ok(ConsumeOk, Consume, State) -> ok_error()
     %% where
     %%      ConsumeOk = #'basic.consume_ok'{}
     %%      Consume = #'basic.consume'{}
     %%      State = state()
     %%
     %% This callback is invoked by the channel every time a
     %% basic.consume_ok is received from the server. Consume is the original
     %% method sent out to the server - it can be used to associate the
     %% call with the response.
     {handle_consume_ok, 3},

     %% handle_cancel(Cancel, State) -> ok_error()
     %% where
     %%      Cancel = #'basic.cancel'{}
     %%      State = state()
     %%
     %% This callback is invoked by the channel every time a basic.cancel
     %% is received from the server.
     {handle_cancel, 2},

     %% handle_cancel_ok(CancelOk, Cancel, State) -> ok_error()
     %% where
     %%      CancelOk = #'basic.cancel_ok'{}
     %%      Cancel = #'basic.cancel'{}
     %%      State = state()
     %%
     %% This callback is invoked by the channel every time a basic.cancel_ok
     %% is received from the server.
     {handle_cancel_ok, 3},

     %% handle_deliver(Deliver, Message, State) -> ok_error()
     %% where
     %%      Deliver = #'basic.deliver'{}
     %%      Message = #amqp_msg{}
     %%      State = state()
     %%
     %% This callback is invoked by the channel every time a basic.deliver
     %% is received from the server.
     {handle_deliver, 3},

     %% handle_info(Info, State) -> ok_error()
     %% where
     %%      Info = any()
     %%      State = state()
     %%
     %% This callback is invoked the consumer process receives a
     %% message.
     {handle_info, 2},

     %% handle_call(Msg, From, State) -> {reply, Reply, NewState} |
     %%                                  {noreply, NewState} |
     %%                                  {error, Reason, NewState}
     %% where
     %%      Msg = any()
     %%      From = any()
     %%      Reply = any()
     %%      State = state()
     %%      NewState = state()
     %%
     %% This callback is invoked by the channel when calling
     %% amqp_channel:call_consumer/2. Reply is the term that
     %% amqp_channel:call_consumer/2 will return. If the callback
     %% returns {noreply, _}, then the caller to
     %% amqp_channel:call_consumer/2 and the channel remain blocked
     %% until gen_server2:reply/2 is used with the provided From as
     %% the first argument.
     {handle_call, 3},

     %% terminate(Reason, State) -> any()
     %% where
     %%      Reason = any()
     %%      State = state()
     %%
     %% This callback is invoked by the channel after it has shut down and
     %% just before its process exits.
     {terminate, 2}
    ];
behaviour_info(_Other) ->
    undefined.

%%---------------------------------------------------------------------------
%% gen_server2 callbacks
%%---------------------------------------------------------------------------

init([ConsumerModule, ExtraParams]) ->
    case ConsumerModule:init(ExtraParams) of
        {ok, MState} ->
            {ok, #state{module = ConsumerModule, module_state = MState}};
        {stop, Reason} ->
            {stop, Reason};
        ignore ->
            ignore
    end.

prioritise_info({'DOWN', _MRef, process, _Pid, _Info}, _State) -> 1;
prioritise_info(_, _State)                                     -> 0.

handle_call({consumer_call, Msg}, From,
            State = #state{module       = ConsumerModule,
                           module_state = MState}) ->
    case ConsumerModule:handle_call(Msg, From, MState) of
        {noreply, NewMState} ->
            {noreply, State#state{module_state = NewMState}};
        {reply, Reply, NewMState} ->
            {reply, Reply, State#state{module_state = NewMState}};
        {error, Reason, NewMState} ->
            {stop, {error, Reason}, {error, Reason},
             State#state{module_state = NewMState}}
    end;
handle_call({consumer_call, Method, Args}, _From,
            State = #state{module       = ConsumerModule,
                           module_state = MState}) ->
    Return =
        case Method of
            #'basic.consume'{} ->
                ConsumerModule:handle_consume(Method, Args, MState);
            #'basic.consume_ok'{} ->
                ConsumerModule:handle_consume_ok(Method, Args, MState);
            #'basic.cancel'{} ->
                ConsumerModule:handle_cancel(Method, MState);
            #'basic.cancel_ok'{} ->
                ConsumerModule:handle_cancel_ok(Method, Args, MState);
            #'basic.deliver'{} ->
                ConsumerModule:handle_deliver(Method, Args, MState)
        end,
    case Return of
        {ok, NewMState} ->
            {reply, ok, State#state{module_state = NewMState}};
        {error, Reason, NewMState} ->
            {stop, {error, Reason}, {error, Reason},
             State#state{module_state = NewMState}}
    end.

handle_cast(_What, State) ->
    {noreply, State}.

handle_info(Info, State = #state{module_state = MState,
                                 module       = ConsumerModule}) ->
    case ConsumerModule:handle_info(Info, MState) of
        {ok, NewMState} ->
            {noreply, State#state{module_state = NewMState}};
        {error, Reason, NewMState} ->
            {stop, {error, Reason}, {error, Reason},
             State#state{module_state = NewMState}}
    end.

terminate(Reason, #state{module = ConsumerModule, module_state = MState}) ->
    ConsumerModule:terminate(Reason, MState).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
