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

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-export([conserve_resources/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("include/rabbit_mqtt_frame.hrl").
-include("include/rabbit_mqtt.hrl").

%%----------------------------------------------------------------------------

start_link(Params) ->
    gen_server2:start_link(?MODULE, Params, []).

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

init({Sock0, SockTransform}) ->
    process_flag(trap_exit, true),
    {ok, Sock} = SockTransform(Sock0),
    ok = rabbit_misc:throw_on_error(
           inet_error, fun () -> rabbit_net:tune_buffer_size(Sock) end),
    {ok, ConnStr} = rabbit_net:connection_string(Sock, inbound),
    log(info, "accepting MQTT connection (~s)~n", [ConnStr]),
    rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    {ok,
     control_throttle(
       #state{ socket        = Sock,
               conn_name     = ConnStr,
               await_recv    = false,
               credit_flow   = running,
               conserve      = false,
               unacked_pubs  = gb_trees:empty(),
               awaiting_ack  = gb_trees:empty(),
               message_id    = 1,
               subscriptions = dict:new(),
               consumer_tags = {undefined, undefined},
               channels      = {undefined, undefined},
               exchange      = rabbit_mqtt_util:env(exchange),
               parse_state   = rabbit_mqtt_frame:initial_state(),
               adapter_info  = adapter_info(Sock) }),
     hibernate, {backoff, 1000, 1000, 10000}}.

handle_call(duplicate_id, _From,
            State = #state{ client_id = ClientId,
                            conn_name = ConnName }) ->
    log(warning, "MQTT disconnecting duplicate client id ~p (~p)~n",
                 [ClientId, ConnName]),
    stop({shutdown, duplicate_id}, State);

handle_call(Msg, From, State) ->
    stop({mqtt_unexpected_call, Msg, From}, State).

handle_cast(Msg, State) ->
    stop({mqtt_unexpected_cast, Msg}, State).

handle_info({#'basic.deliver'{}, #amqp_msg{}} = Delivery, State) ->
    rabbit_mqtt_processor:amqp_callback(Delivery, State);

handle_info(#'basic.ack'{} = Ack, State) ->
    rabbit_mqtt_processor:amqp_callback(Ack, State);

handle_info(#'basic.nack'{ delivery_tag = Tag },
            State = #state { client_id = ClientId,
                             unacked_pubs = UnackedPubs }) ->
    % todo: store and republish message
    MsgId = gb_trees:get(Tag, UnackedPubs),
    log(error, "MQTT received nack for client id ~p, msg id ~p~n",
               [ClientId, MsgId]),
    {stop, {nack_received, MsgId}, State};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};

handle_info(#'basic.cancel'{}, State) ->
    stop({shutdown, subscription_cancelled}, State);

handle_info({'EXIT', Conn, Reason}, State = #state{ connection = Conn }) ->
    stop({amqp_conn_died, Reason}, State);

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}}, State = #state { socket = Sock }) ->
    process_received_bytes(
      Data, control_throttle(State #state{ await_recv = false }));

handle_info({Inet, _Sock, _Ref, {error, closed}},
            State = #state { client_id = ClientId,
                             will_msg  = WillMsg })
  when Inet =:= inet_async orelse Inet =:= inet_reply ->
    log(info, "MQTT detected network error for ~p~n", [ClientId]),
    rabbit_mqtt_processor:amqp_pub(WillMsg, State),
    % todo: flush channel after publish
    stop({shutdown, conn_closed}, State);

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
                       State = #state { parse_state = ParseState }) ->
    case
        rabbit_mqtt_frame:parse(Bytes, ParseState) of
            {more, ParseState1} ->
                {noreply, control_throttle(
                            State #state{ parse_state = ParseState1 })};
            {ok, Frame, Rest} ->
                case rabbit_mqtt_processor:process_frame(Frame, State) of
                    {ok, State1} ->
                        PS = rabbit_mqtt_frame:initial_state(),
                        process_received_bytes(Rest,
                                               State1#state{ parse_state = PS });
                    {stop, Reason, State1} ->
                        stop(Reason, State1)
                end;
            {error, Error} ->
                log(error, "MQTT detected framing error ~p~n", [Error]),
                stop({shutdown, Error}, State)
    end.

%%----------------------------------------------------------------------------

stop(Reason, State = #state { client_id = undefined }) ->
    {stop, Reason, close_connection(State)};

stop(Reason, State = #state { client_id = ClientId }) ->
    % todo: maybe clean session
    rabbit_mqtt_collector:unregister(ClientId),
    {stop, Reason, close_connection(State)}.

close_connection(State = #state{ connection = undefined }) ->
    State;
close_connection(State = #state{ connection = Connection }) ->
    %% ignore noproc or other exceptions to avoid debris
    catch amqp_connection:close(Connection),
    State #state{ channels   = {undefined, undefined},
                  connection = undefined }.

adapter_info(Sock) ->
    {Addr, Port} = case rabbit_net:sockname(Sock) of
                       {ok, Res} -> Res;
                       _         -> {unknown, unknown}
                   end,
    {PeerAddr, PeerPort} = case rabbit_net:peername(Sock) of
                               {ok, Res2} -> Res2;
                               _          -> {unknown, unknown}
                           end,
    Name = case rabbit_net:connection_string(Sock, inbound) of
               {ok, Res3} -> Res3;
               _          -> unknown
           end,
    #adapter_info{ protocol     = {'MQTT', {?MQTT_PROTO_MAJOR,
                                            ?MQTT_PROTO_MINOR}},
                   name         = list_to_binary(Name),
                   address      = Addr,
                   port         = Port,
                   peer_address = PeerAddr,
                   peer_port    = PeerPort}.

run_socket(State = #state{ credit_flow = blocked }) ->
    State;
run_socket(State = #state{ await_recv = true }) ->
    State;
run_socket(State = #state{ socket = Sock }) ->
    rabbit_net:async_recv(Sock, 0, infinity),
    State#state{ await_recv = true }.

conserve_resources(Pid, _, Conserve) ->
    Pid ! {conserve_resources, Conserve},
    ok.

control_throttle(State = #state{ credit_flow = Flow,
                                 conserve    = Conserve }) ->
    case {Flow, Conserve orelse credit_flow:blocked()} of
        {running,   true} -> State #state{ credit_flow = blocking };
        {blocking, false} -> run_socket(State #state{ credit_flow = running });
        {blocked,  false} -> run_socket(State #state{ credit_flow = running });
        {_,            _} -> run_socket(State)
    end.
