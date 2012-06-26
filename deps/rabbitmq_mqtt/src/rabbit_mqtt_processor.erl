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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_processor).
-behaviour(gen_server2).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {session_id, channel, connection, subscriptions,
                config, adapter_info, send_fun}).

-define(FLUSH_TIMEOUT, 60000).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, []).

%%----------------------------------------------------------------------------
%% Basic gen_server2 callbacks
%%----------------------------------------------------------------------------

init([SendFun, AdapterInfo, Configuration]) ->
    process_flag(trap_exit, true),
    {ok,
     #state {
       send_fun      = SendFun,
       channel       = none,
       connection    = none,
       subscriptions = dict:new(),
       config        = Configuration,
       adapter_info  = AdapterInfo},
     hibernate,
     {backoff, 1000, 1000, 10000}
    }.

terminate(_Reason, _State) ->
    ok.

handle_cast(_Cast, State) -> State.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info({inet_reply, _, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, State}.

%%----------------------------------------------------------------------------
%% Success/error handling
%%----------------------------------------------------------------------------

log_error(Message, Detail, ServerPrivateDetail) ->
    rabbit_log:error("MQTT error:~n"
                     "Message: ~p~n"
                     "Detail: ~p~n"
                     "Server private detail: ~p~n",
                     [Message, Detail, ServerPrivateDetail]).


%%----------------------------------------------------------------------------
%% Skeleton gen_server2 callbacks
%%----------------------------------------------------------------------------
handle_call(_Msg, _From, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
