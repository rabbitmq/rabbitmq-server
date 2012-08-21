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

-module(rabbit_mqtt_reader).
-behaviour(gen_server2).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-export([conserve_resources/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("include/rabbit_mqtt.hrl").

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link(?MODULE, [], []).

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

init([]) ->
    {ok, undefined, hibernate, {backoff, 1000, 1000, 10000}}.

handle_call({go, Sock0, SockTransform}, _From, undefined) ->
    process_flag(trap_exit, true),
    {ok, Sock} = SockTransform(Sock0),
    ok = rabbit_misc:throw_on_error(
           inet_error, fun () -> rabbit_net:tune_buffer_size(Sock) end),
    {ok, ConnStr} = rabbit_net:connection_string(Sock, inbound),
    log(info, "accepting MQTT connection (~s)~n", [ConnStr]),
    rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    {reply, ok,
     control_throttle(
       #state{ socket           = Sock,
               conn_name        = ConnStr,
               await_recv       = false,
               connection_state = running,
               conserve         = false,
               parse_state      = rabbit_mqtt_frame:initial_state(),
               proc_state       = rabbit_mqtt_processor:initial_state(Sock) })};

handle_call(duplicate_id, _From,
            State = #state{ proc_state = PState,
                            conn_name  = ConnName }) ->
    log(warning, "MQTT disconnecting duplicate client id ~p (~p)~n",
                 [rabbit_mqtt_processor:info(client_id, PState), ConnName]),
    stop({shutdown, duplicate_id}, State);

handle_call(Msg, From, State) ->
    stop({mqtt_unexpected_call, Msg, From}, State).

handle_cast(Msg, State) ->
    stop({mqtt_unexpected_cast, Msg}, State).

handle_info({#'basic.deliver'{}, #amqp_msg{}} = Delivery,
            State = #state{ proc_state = ProcState }) ->
    callback_reply(State, rabbit_mqtt_processor:amqp_callback(Delivery, ProcState));

handle_info(#'basic.ack'{} = Ack, State = #state{ proc_state = ProcState }) ->
    callback_reply(State, rabbit_mqtt_processor:amqp_callback(Ack, ProcState));

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};

handle_info(#'basic.cancel'{}, State) ->
    stop({shutdown, subscription_cancelled}, State);

handle_info({'EXIT', _Conn, Reason}, State) ->
    stop({connection_died, Reason}, State);

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}},
            State = #state{ socket = Sock }) ->
    process_received_bytes(
      Data, control_throttle(State #state{ await_recv = false }));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State = #state {}) ->
    network_error(Reason, State);

handle_info({inet_reply, _Sock, {error, Reason}}, State = #state {}) ->
    network_error(Reason, State);

handle_info({conserve_resources, Conserve}, State) ->
    {noreply, control_throttle(State #state{ conserve = Conserve }), hibernate};

handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    {noreply, control_throttle(State), hibernate};

handle_info(Msg, State) ->
    stop({mqtt_unexpected_msg, Msg}, State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

process_received_bytes(<<>>, State) ->
    {noreply, State};
process_received_bytes(Bytes,
                       State = #state{ parse_state = ParseState,
                                       proc_state  = ProcState,
                                       conn_name   = ConnStr }) ->
    case
        rabbit_mqtt_frame:parse(Bytes, ParseState) of
            {more, ParseState1} ->
                {noreply,
                 control_throttle( State #state{ parse_state = ParseState1 }),
                 hibernate};
            {ok, Frame, Rest} ->
                case rabbit_mqtt_processor:process_frame(Frame, ProcState) of
                    {ok, ProcState1} ->
                        PS = rabbit_mqtt_frame:initial_state(),
                        process_received_bytes(
                          Rest,
                          State #state{ parse_state = PS,
                                        proc_state = ProcState1 });
                    {err, Reason, ProcState1} ->
                        log:info("MQTT protocol error ~p for connection ~p~n",
                                 [Reason, ConnStr]),
                        stop({shutdown, Reason}, pstate(State, ProcState1));
                    {stop, ProcState1} ->
                        stop(normal, pstate(State, ProcState1))
                end;
            {error, Error} ->
                log(error, "MQTT detected framing error ~p for connection ~p~n",
                           [ConnStr, Error]),
                stop({shutdown, Error}, State)
    end.

callback_reply(State, {ok, ProcState}) ->
    {noreply, pstate(State, ProcState), hibernate};
callback_reply(State, {err, Reason, ProcState}) ->
    {stop, Reason, pstate(State, ProcState)}.

pstate(State = #state {}, PState = #proc_state{}) ->
    State #state{ proc_state = PState }.

%%----------------------------------------------------------------------------

network_error(Reason,
              State = #state{ conn_name  = ConnStr,
                              proc_state = PState }) ->
    log(info, "MQTT detected network error for ~p~n", [ConnStr]),
    rabbit_mqtt_processor:send_will(PState),
    % todo: flush channel after publish
    stop({shutdown, conn_closed}, State).

stop(Reason, State = #state{}) ->
    {stop, Reason, close_connection(State)};

stop(Reason, State = #state{ proc_state = PState }) ->
    % todo: maybe clean session
    ok = rabbit_mqtt_collector:unregister(
           rabbit_mqtt_processor:info(client_info, PState)),
    {stop, Reason, close_connection(State)}.

close_connection(State = #state{ proc_state = ProcState} ) ->
    pstate(State, rabbit_mqtt_processor:close_connection(ProcState)).

run_socket(State = #state{ connection_state = blocked }) ->
    State;
run_socket(State = #state{ await_recv = true }) ->
    State;
run_socket(State = #state{ socket = Sock }) ->
    rabbit_net:async_recv(Sock, 0, infinity),
    State#state{ await_recv = true }.

conserve_resources(Pid, _, Conserve) ->
    Pid ! {conserve_resources, Conserve},
    ok.

control_throttle(State = #state{ connection_state = Flow,
                                 conserve         = Conserve }) ->
    case {Flow, Conserve orelse credit_flow:blocked()} of
        {running,   true} -> State #state{ connection_state = blocked };
        {blocked,  false} -> run_socket(State #state{
                                                connection_state = running });
        {_,            _} -> run_socket(State)
    end.
