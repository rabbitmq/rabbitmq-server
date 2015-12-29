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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2015 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_web_mqtt_handler).

-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {
    conn_name,
    parse_state,
    proc_state
}).

init(_, _, _) ->
    {upgrade, protocol, cowboy_websocket}.

websocket_init(_, Req, _) ->
%    io:format(user, "~p~n", [Req]),

    process_flag(trap_exit, true),
    Sock = cowboy_req:get(socket, Req),
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            rabbit_log:log(connection, info, "accepting WEB-MQTT connection ~p (~s)~n", [self(), ConnStr]),
%            rabbit_alarm:register(
%              self(), {?MODULE, conserve_resources, []}),
            ProcessorState = rabbit_mqtt_processor:initial_state(Sock,
                rabbit_mqtt_reader:ssl_login_name(Sock),
                fun send_reply/2),
            {SecWsProtocol, Req1} = cowboy_req:header(<<"sec-websocket-protocol">>, Req),
            Req2 = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, SecWsProtocol, Req1),
            %% @todo compact?
            %% control_throttle
            {ok, Req2, #state{
                %% keepalive/keepalive_sup
                conn_name   = ConnStr,
                parse_state = rabbit_mqtt_frame:initial_state(),
                proc_state  = ProcessorState
            }};
        _ ->
            {shutdown, Req}
    end.

%% @todo hibernate everywhere?
websocket_handle({binary, Data}, Req, State) ->
    handle_data(Data, Req, State);
websocket_handle(Frame, Req, State) ->
    rabbit_log:info("rabbit_web_mqtt: unexpected Websocket frame ~p~n",
                    [Frame]),
    {ok, Req, State}.

websocket_info({#'basic.deliver'{}, #amqp_msg{}, _DeliveryCtx} = Delivery,
            Req, State = #state{ proc_state = ProcState0 }) ->
    case rabbit_mqtt_processor:amqp_callback(Delivery, ProcState0) of
        {ok, ProcState} ->
            {ok, Req, State #state { proc_state = ProcState }};
        {error, _, _} ->
            {shutdown, Req, State}
    end;
websocket_info(#'basic.ack'{} = Ack, Req, State = #state{ proc_state = ProcState0 }) ->
    case rabbit_mqtt_processor:amqp_callback(Ack, ProcState0) of
        {ok, ProcState} ->
            {ok, Req, State #state { proc_state = ProcState }};
        {error, _, _} ->
            {shutdown, Req, State}
    end;
websocket_info(#'basic.consume_ok'{}, Req, State) ->
    {ok, Req, State};
websocket_info(#'basic.cancel'{}, Req, State) ->
    {shutdown, Req, State};
websocket_info({reply, Data}, Req, State) ->
    {reply, {binary, Data}, Req, State};
websocket_info({'EXIT', _, _}, State) ->
    {shutdown, Req, State};
websocket_info({'$gen_cast', duplicate_id}, Req, State = #state{ proc_state = ProcState,
                                                                 conn_name = ConnName }) ->
    rabbit_log:warning("MQTT disconnecting duplicate client id ~p (~p)~n",
                 [rabbit_mqtt_processor:info(client_id, ProcState), ConnName]),
    {shutdown, Req, State};
websocket_info(Msg, Req, State) ->
    rabbit_log:info("rabbit_web_mqtt: unexpected message ~p~n",
                    [Msg]),
    {ok, Req, State}.


websocket_terminate(_, _, #state{proc_state=ProcState}) ->
    rabbit_mqtt_processor:close_connection(ProcState),
    ok;
websocket_terminate(_, _, _) ->
    ok.

%% Internal.

handle_data(<<>>, Req, State) ->
    {ok, Req, State};
handle_data(Data, Req, State = #state{ parse_state = ParseState,
                                       proc_state  = ProcState,
                                       conn_name   = ConnStr }) ->
    case rabbit_mqtt_frame:parse(Data, ParseState) of
        {more, ParseState1} ->
            %% @todo control_throttle
            {ok, Req, State #state{ parse_state = ParseState1 }};
        {ok, Frame, Rest} ->
            case rabbit_mqtt_processor:process_frame(Frame, ProcState) of
                {ok, ProcState1} ->
                    PS = rabbit_mqtt_frame:initial_state(),
                    handle_data(
                      Rest, Req,
                      State #state{ parse_state = PS,
                                    proc_state = ProcState1 });
                {error, Reason, _} ->
                    rabbit_log:log(connection, info, "MQTT protocol error ~p for connection ~p~n",
                        [Reason, ConnStr]),
                    {shutdown, Req, State};
                {error, Error} ->
                    rabbit_log:log(connection, error, "MQTT detected framing error '~p' for connection ~p~n",
                        [Error, ConnStr]),
                    {shutdown, Req, State};
                {stop, _} ->
                    {shutdown, Req, State}
            end;
        {error, Error} ->
            rabbit_log:log(connection, error, "MQTT detected framing error '~p' for connection ~p~n",
                [ConnStr, Error]),
            {shutdown, Req, State}
    end.

send_reply(Frame, _) ->
    rabbit_log:info("MQTT sending frame ~p ~n", [Frame]),
    self() ! {reply, rabbit_mqtt_frame:serialise(Frame)}.
