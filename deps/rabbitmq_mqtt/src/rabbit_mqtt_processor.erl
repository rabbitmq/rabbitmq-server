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

-export([start_link/1, process_frame/2, flush_and_die/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("include/rabbit_mqtt_frame.hrl").

-record(state, {session_id,
                channel,
                connection,
                adapter_info,
                send_fun
               }).

-define(MQTT_PROTOCOL_VERSION, 3).
-define(FLUSH_TIMEOUT, 60000).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, []).

process_frame(Pid, Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PUBLISH }} ) ->
    credit_flow:send(Pid),
    gen_server2:cast(Pid, {?PUBLISH, Frame, self()});
process_frame(Pid, Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }} ) ->
    gen_server2:cast(Pid, {Type, Frame, noflow}).

flush_and_die(Pid) ->
    gen_server2:cast(Pid, flush_and_die).

%%----------------------------------------------------------------------------
%% Basic gen_server2 callbacks
%%----------------------------------------------------------------------------

init([SendFun, AdapterInfo, Configuration]) ->
    process_flag(trap_exit, true),
    {ok,
     #state {
       session_id          = none,
       channel             = none,
       connection          = none,
       adapter_info        = AdapterInfo,
       send_fun            = SendFun
       },
     hibernate,
     {backoff, 1000, 1000, 10000}
    }.

terminate(_Reason, State) ->
    close_connection(State).

handle_cast(flush_and_die, State) ->
    {stop, normal, close_connection(State)};

handle_cast({?CONNECT, Frame, noflow}, State) ->
    process_connect(Frame, State);

handle_cast({_Type, Frame, _FlowPid}, State = #state{channel = none}) ->
    rabbit_log:error("Ignoring invalid MQTT frame prior to login: ~p~n", [Frame]),
    {noreply, State, hibernate};

handle_cast({Command, Frame, FlowPid}, State) ->
    case FlowPid of
        noflow -> ok;
        _      -> credit_flow:ack(FlowPid)
    end,
    State;

handle_cast(client_timeout, State) ->
    {stop, client_timeout, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info({'EXIT', Conn, Reason}, State = #state{connection = Conn}) ->
    {stop, {conn_died, Reason}, State};
handle_info({inet_reply, _, ok}, State) ->
    {noreply, State, hibernate};
handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    {noreply, State, hibernate};

handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, State}.

process_connect(#mqtt_frame{} = Frame,
                State = #state{channel        = none}) ->
    {ok, VHost} = application:get_env(rabbitmq_mqtt, vhost),
    {ok, DefaultUser} = application:get_env(rabbitmq_mqtt, default_user),
    {ok, DefaultPass} = application:get_env(rabbitmq_mqtt, default_pass),
    send_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed {type = ?CONNACK},
                            variable = #mqtt_frame_connack { return_code = ?CONNACK_ACCEPT }},
               State),
    {noreply, State, hibernate}.

send_frame(Frame, State = #state{send_fun = SendFun}) ->
    SendFun(async, rabbit_mqtt_frame:serialise(Frame)),
    State.

%% Closing the connection will close the channel and subchannels
close_connection(State = #state{connection = Connection}) ->
    %% ignore noproc or other exceptions to avoid debris
    catch amqp_connection:close(Connection),
    State#state{channel = none, connection = none}.

%%----------------------------------------------------------------------------
%% Skeleton gen_server2 callbacks
%%----------------------------------------------------------------------------
handle_call(_Msg, _From, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

