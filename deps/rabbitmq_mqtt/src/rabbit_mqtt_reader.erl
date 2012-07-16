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

-export([send_frame/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("include/rabbit_mqtt_frame.hrl").
-include("include/rabbit_mqtt.hrl").

%%----------------------------------------------------------------------------

start_link(Configuration) ->
    gen_server2:start_link(?MODULE, Configuration, []).

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

init(Configuration) ->
    {ok, #state {},
     hibernate,
     {backoff, 1000, 1000, 10000}}.

handle_call({go, Sock}, _From, #state { socket = undefined } ) ->
    {ok, ConnStr} = rabbit_net:connection_string(Sock, inbound),
    log(info, "accepting MQTT connection ~p (~s)~n", [self(), ConnStr]),
    ParseState = rabbit_mqtt_frame:initial_state(),
    {ok, _Ref} = rabbit_net:async_recv(Sock, 0, infinity),
    {reply, ok, #state { socket       = Sock,
                         conn_name    = ConnStr,
                         unacked_pubs = queue:new(),
                         confirms     = false,
                         parse_state  = ParseState,
                         adapter_info = adapter_info(Sock) }};

handle_call(Msg, From, State) ->
    stop({mqtt_unexpected_call, Msg, From}, State).

handle_cast(Msg, State) ->
    stop({mqtt_unexpected_cast, Msg}, State).

handle_info({#'basic.deliver'{delivery_tag = Tag,
                              routing_key = RoutingKey },
             #amqp_msg{ payload = Payload }},
            #state { channel    = Channel,
                     socket     = Sock } = State ) ->
    send_frame(
      Sock,
      #mqtt_frame{ fixed    = #mqtt_frame_fixed {type = ?PUBLISH },
                   variable = #mqtt_frame_publish {
                                topic_name = rabbit_mqtt_util:untranslate_topic(
                                               RoutingKey) },
                   payload = Payload}),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State, hibernate};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};

handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State, hibernate};

handle_info(#'basic.ack'{ multiple = Multiple, delivery_tag = Tag } = Ack,
            State = #state { socket = Sock,
                             unacked_pubs = UnackedPubs }) ->
    case queue:out(UnackedPubs) of
        {{value, {SeqNo, MsgId}}, UnackedPubs1} ->
            send_frame(
              Sock,
              #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PUBACK },
                           variable = #mqtt_frame_publish{ message_id =
                                                             MsgId }}),
            State1 = State #state { unacked_pubs = UnackedPubs1 },
            case Multiple of
                true -> handle_info(Ack, State1);
                false -> {noreply, State1, hibernate}
            end;
        {empty, _} ->
            {noreply, State, hibernate}
    end;

handle_info(#'basic.nack'{}, State = #state { unacked_pubs = UnackedPubs }) ->
    % todo: republish message
    log(error, "MQTT received nack for msg id ~p~n", [queue:get(UnackedPubs)]),
    {stop, nack, State};

handle_info({'EXIT', Conn, Reason}, State = #state{ connection = Conn }) ->
    stop({conn_died, Reason}, State);

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}},
            State = #state { socket = Sock }) ->
    rabbit_net:async_recv(Sock, 0, infinity),
    process_received_bytes(Data, State);

handle_info({inet_async, _Sock, _Ref, {error, closed}}, State) ->
    stop({shutdown, conn_closed}, close_connection(State));

handle_info(Msg, State) ->
    stop({mqtt_unexpected_msg, Msg}, State).

terminate(Reason, State) ->
    stop(Reason, State).

process_received_bytes(<<>>, State) ->
    {noreply, State};
process_received_bytes(Bytes,
                       State = #state { parse_state = ParseState }) ->
    case
        rabbit_mqtt_frame:parse(Bytes, ParseState) of
            {more, ParseState1} ->
                {noreply, State #state{ parse_state = ParseState1 }};
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
                rabbit_log:error("MQTT framing error ~p~n", [Error]),
                stop({shutdown, Error}, State)
    end.


%%----------------------------------------------------------------------------

stop(Reason, State = #state { conn_name = ConnStr }) ->
    % todo: maybe clean session
    % todo: execute last will
    log(info, "closing MQTT connection ~p (~s)~n", [self(), ConnStr]),
    {stop, Reason, close_connection(State)}.

%% Closing the connection will close the channel and subchannels
close_connection(State = #state{connection = Connection})
  when Connection =/= undefined ->
    %% ignore noproc or other exceptions to avoid debris
    catch amqp_connection:close(Connection),
    State#state{channel = undefined, connection = undefined};
close_connection(State) ->
    State.

send_frame(Sock, Frame) ->
    %rabbit_log:error("sending frame ~p ~n", [Frame]),
    rabbit_net:port_command(Sock, rabbit_mqtt_frame:serialise(Frame)).

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
    #adapter_info{protocol     = {'MQTT', {?MQTT_PROTO_MAJOR, ?MQTT_PROTO_MINOR}},
                  name         = list_to_binary(Name),
                  address      = Addr,
                  port         = Port,
                  peer_address = PeerAddr,
                  peer_port    = PeerPort}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
