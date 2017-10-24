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
%% Copyright (c) 2012-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_web_mqtt_handler).
-behaviour(cowboy_websocket).

-export([init/2]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).
-export([terminate/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {
    conn_name,
    keepalive,
    keepalive_sup,
    parse_state,
    proc_state,
    socket,
    stats_timer,
    connection
}).

init(Req, Opts) ->
    {_, KeepaliveSup} = lists:keyfind(keepalive_sup, 1, Opts),
    {_, Sock} = lists:keyfind(socket, 1, Opts),
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            Req2 = case cowboy_req:header(<<"sec-websocket-protocol">>, Req) of
                undefined -> Req;
                SecWsProtocol ->
                    cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, SecWsProtocol, Req)
            end,
            {cowboy_websocket, Req2, #state{
                conn_name     = ConnStr,
                keepalive     = {none, none},
                keepalive_sup = KeepaliveSup,
                parse_state   = rabbit_mqtt_frame:initial_state(),
                socket        = Sock
            }};
        _ ->
            {stop, Req}
    end.

websocket_init(State = #state{conn_name = ConnStr, socket = Sock}) ->
    rabbit_log_connection:info("accepting Web MQTT connection ~p (~s)~n", [self(), ConnStr]),
    AdapterInfo0 = #amqp_adapter_info{additional_info=Extra}
        = amqp_connection:socket_adapter_info(Sock, {'Web MQTT', "N/A"}),
    %% Flow control is not supported for Web-MQTT connections.
    AdapterInfo = AdapterInfo0#amqp_adapter_info{
        additional_info=[{state, running}|Extra]},
    ProcessorState = rabbit_mqtt_processor:initial_state(Sock,
        rabbit_mqtt_reader:ssl_login_name(Sock),
        AdapterInfo,
        fun send_reply/2),
    process_flag(trap_exit, true),
    {ok,
     rabbit_event:init_stats_timer(
         State#state{proc_state = ProcessorState},
         #state.stats_timer),
     hibernate}.

websocket_handle({binary, Data}, State) ->
    handle_data(Data, State);
websocket_handle(Frame, State) ->
    rabbit_log_connection:info("Web MQTT: unexpected WebSocket frame ~p~n",
                    [Frame]),
    {ok, State, hibernate}.

websocket_info({#'basic.deliver'{}, #amqp_msg{}, _DeliveryCtx} = Delivery,
            State = #state{ proc_state = ProcState0 }) ->
    case rabbit_mqtt_processor:amqp_callback(Delivery, ProcState0) of
        {ok, ProcState} ->
            {ok, State #state { proc_state = ProcState }, hibernate};
        {error, _, _} ->
            {stop, State}
    end;
websocket_info(#'basic.ack'{} = Ack, State = #state{ proc_state = ProcState0 }) ->
    case rabbit_mqtt_processor:amqp_callback(Ack, ProcState0) of
        {ok, ProcState} ->
            {ok, State #state { proc_state = ProcState }, hibernate};
        {error, _, _} ->
            {stop, State}
    end;
websocket_info(#'basic.consume_ok'{}, State) ->
    {ok, State, hibernate};
websocket_info(#'basic.cancel'{}, State) ->
    {stop, State};
websocket_info({reply, Data}, State) ->
    {reply, {binary, Data}, State, hibernate};
websocket_info({'EXIT', _, _}, State) ->
    {stop, State};
websocket_info({'$gen_cast', duplicate_id}, State = #state{ proc_state = ProcState,
                                                                 conn_name = ConnName }) ->
    rabbit_log_connection:warning("WEB-MQTT disconnecting duplicate client id ~p (~p)~n",
                 [rabbit_mqtt_processor:info(client_id, ProcState), ConnName]),
    {stop, State};
websocket_info({start_keepalives, Keepalive},
               State = #state{ socket = Sock, keepalive_sup = KeepaliveSup }) ->
    %% Only the client has the responsibility for sending keepalives
    SendFun = fun() -> ok end,
    Parent = self(),
    ReceiveFun = fun() -> Parent ! keepalive_timeout end,
    Heartbeater = rabbit_heartbeat:start(
                    KeepaliveSup, Sock, 0, SendFun, Keepalive, ReceiveFun),
    {ok, State #state { keepalive = Heartbeater }};
websocket_info(keepalive_timeout, State = #state {conn_name = ConnStr,
                                                       proc_state = PState}) ->
    rabbit_log_connection:error("closing WEB-MQTT connection ~p (keepalive timeout)~n", [ConnStr]),
    rabbit_mqtt_processor:send_will(PState),
    {stop, State};
websocket_info(emit_stats, State) ->
    {ok, emit_stats(State)};
websocket_info(Msg, State) ->
    rabbit_log_connection:info("WEB-MQTT: unexpected message ~p~n",
                    [Msg]),
    {ok, State, hibernate}.

terminate(_, _, State = #state{ proc_state = ProcState,
                                conn_name  = ConnName }) ->
    maybe_emit_stats(State),
    rabbit_log_connection:info("closing WEB-MQTT connection ~p (~s)~n", [self(), ConnName]),
    rabbit_mqtt_processor:close_connection(ProcState),
    ok;
terminate(_, _, State) ->
    maybe_emit_stats(State),
    ok.

%% Internal.

handle_data(<<>>, State) ->
    {ok, ensure_stats_timer(State), hibernate};
handle_data(Data, State = #state{ parse_state = ParseState,
                                       proc_state  = ProcState,
                                       conn_name   = ConnStr }) ->
    case rabbit_mqtt_frame:parse(Data, ParseState) of
        {more, ParseState1} ->
            {ok, ensure_stats_timer(State #state{ parse_state = ParseState1 }), hibernate};
        {ok, Frame, Rest} ->
            case rabbit_mqtt_processor:process_frame(Frame, ProcState) of
                {ok, ProcState1, ConnPid} ->
                    PS = rabbit_mqtt_frame:initial_state(),
                    handle_data(
                      Rest,
                      State #state{ parse_state = PS,
                                    proc_state = ProcState1,
                                    connection = ConnPid });
                {error, Reason, _} ->
                    rabbit_log_connection:info("MQTT protocol error ~p for connection ~p~n",
                        [Reason, ConnStr]),
                    {stop, State};
                {error, Error} ->
                    rabbit_log_connection:error("MQTT detected framing error '~p' for connection ~p~n",
                        [Error, ConnStr]),
                    {stop, State};
                {stop, _} ->
                    {stop, State}
            end;
        {error, Error} ->
            rabbit_log_connection:error("MQTT detected framing error '~p' for connection ~p~n",
                [ConnStr, Error]),
            {stop, State}
    end.

send_reply(Frame, _) ->
    self() ! {reply, rabbit_mqtt_frame:serialise(Frame)}.

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats).

maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #state.stats_timer,
                                fun() -> emit_stats(State) end).

emit_stats(State=#state{connection = C}) when C == none; C == undefined ->
    %% Avoid emitting stats on terminate when the connection has not yet been
    %% established, as this causes orphan entries on the stats database
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    State1;
emit_stats(State=#state{socket=Sock, connection=Conn}) ->
    SockInfos = case rabbit_net:getstat(Sock,
            [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]) of
        {ok,    SI} -> SI;
        {error,  _} -> []
    end,
    Infos = [{pid, Conn}|SockInfos],
    rabbit_core_metrics:connection_stats(Conn, Infos),
    rabbit_event:notify(connection_stats, Infos),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    State1.
