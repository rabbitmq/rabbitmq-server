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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ws_client).
-behaviour(gen_server).

-include_lib("rabbitmq_stomp/include/rabbit_stomp.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/1]).
-export([sockjs_msg/2, sockjs_closed/1]).

-export([init/1, handle_call/3, handle_info/2, terminate/2,
         code_change/3, handle_cast/2]).

-record(state, {conn, proc_state, parse_state, stats_timer, connection, heartbeat_mode, heartbeat, heartbeat_sup}).

%%----------------------------------------------------------------------------

start_link(Params) ->
    gen_server:start_link(?MODULE, Params, []).

sockjs_msg(Pid, Data) ->
    gen_server:cast(Pid, {sockjs_msg, Data}).

sockjs_closed(Pid) ->
    gen_server:cast(Pid, sockjs_closed).

%%----------------------------------------------------------------------------

init({SupPid, Conn, Heartbeat, Conn}) ->
    ok = file_handle_cache:obtain(),
    process_flag(trap_exit, true),
    {ok, ProcessorState} = init_processor_state(Conn),
    {ok, rabbit_event:init_stats_timer(
           #state{conn           = Conn,
                  proc_state     = ProcessorState,
                  parse_state    = rabbit_stomp_frame:initial_state(),
                  heartbeat_sup  = SupPid,
                  heartbeat      = {none, none},
                  heartbeat_mode = Heartbeat},
           #state.stats_timer)}.

init_processor_state(Conn) ->
    SendFun = fun (_Sync, Data) ->
                      Conn:send(Data),
                      ok
              end,
    Info = Conn:info(),
    Headers = proplists:get_value(headers, Info),

    UseHTTPAuth = application:get_env(rabbitmq_web_stomp, use_http_auth, false),

    StompConfig0 = #stomp_configuration{implicit_connect = false},

    StompConfig = case UseHTTPAuth of
        true ->
            case lists:keyfind(authorization, 1, Headers) of
                false ->
                    %% We fall back to the default STOMP credentials.
                    UserConfig = application:get_env(rabbitmq_stomp, default_user, undefined),
                    StompConfig1 = rabbit_stomp:parse_default_user(UserConfig, StompConfig0),
                    StompConfig1#stomp_configuration{force_default_creds = true};
                {_, AuthHd} ->
                    {<<"basic">>, {HTTPLogin, HTTPPassCode}}
                        = cowboy_http:token_ci(list_to_binary(AuthHd),
                                               fun cowboy_http:authorization/2),
                    StompConfig0#stomp_configuration{
                      default_login = HTTPLogin,
                      default_passcode = HTTPPassCode,
                      force_default_creds = true}
            end;
        false ->
            StompConfig0
    end,

    Sock = proplists:get_value(socket, Info),
    {PeerAddr, _} = proplists:get_value(peername, Info),
    AdapterInfo0 = #amqp_adapter_info{additional_info=Extra}
        = amqp_connection:socket_adapter_info(Sock, {'Web STOMP', 0}),
    %% Flow control is not supported for Web-STOMP connections.
    AdapterInfo = AdapterInfo0#amqp_adapter_info{
        additional_info=[{state, running}|Extra]},

    ProcessorState = rabbit_stomp_processor:initial_state(
        StompConfig,
        {SendFun, AdapterInfo, none, PeerAddr}),
    {ok, ProcessorState}.

handle_cast({sockjs_msg, Data}, State = #state{proc_state  = ProcessorState,
                                               parse_state = ParseState,
                                               connection  = ConnPid}) ->
    case process_received_bytes(Data, ProcessorState, ParseState, ConnPid) of
        {ok, NewProcState, ParseState1, ConnPid1} ->
            {noreply, ensure_stats_timer(State#state{
                        parse_state = ParseState1,
                        proc_state  = NewProcState,
                        connection  = ConnPid1})};
        {stop, Reason, NewProcState, ParseState1} ->
            {stop, Reason, State#state{
                                parse_state = ParseState1,
                                proc_state  = NewProcState}}
    end;

handle_cast(sockjs_closed, State) ->
    {stop, normal, State};

handle_cast(client_timeout, State) ->
    {stop, {shutdown, client_heartbeat_timeout}, State};

handle_cast(Cast, State) ->
    {stop, {odd_cast, Cast}, State}.

%% TODO this is a bit rubbish - after the preview release we should
%% make the credit_flow:send/1 invocation in
%% rabbit_stomp_processor:process_frame/2 optional.
handle_info({bump_credit, {_, _}}, State) ->
    {noreply, State};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.ack'{delivery_tag = Tag, multiple = IsMulti}, State) ->
    ProcState = processor_state(State),
    NewProcState = rabbit_stomp_processor:flush_pending_receipts(Tag,
                                                                   IsMulti,
                                                                   ProcState),
    {noreply, processor_state(NewProcState, State)};
handle_info({Delivery = #'basic.deliver'{},
             #amqp_msg{props = Props, payload = Payload},
             DeliveryCtx},
             State) ->
    ProcState = processor_state(State),
    NewProcState = rabbit_stomp_processor:send_delivery(Delivery,
                                                          Props,
                                                          Payload,
                                                          DeliveryCtx,
                                                          ProcState),
    {noreply, processor_state(NewProcState, State)};
handle_info(#'basic.cancel'{consumer_tag = Ctag}, State) ->
    ProcState = processor_state(State),
    case rabbit_stomp_processor:cancel_consumer(Ctag, ProcState) of
      {ok, NewProcState, _Connection} ->
        {noreply, processor_state(NewProcState, State)};
      {stop, Reason, NewProcState} ->
        {stop, Reason, processor_state(NewProcState, State)}
    end;

handle_info({start_heartbeats, _},
            State = #state{heartbeat_mode = no_heartbeat}) ->
    {noreply, State};

handle_info({start_heartbeats, {0, 0}}, State) ->
    {noreply, State};
handle_info({start_heartbeats, {SendTimeout, ReceiveTimeout}},
            State = #state{conn = Conn,
                           heartbeat_sup = SupPid,
                           heartbeat_mode = heartbeat}) ->
    Info = Conn:info(),
    Sock = proplists:get_value(socket, Info),
    Pid = self(),
    SendFun = fun () -> Conn:send(<<$\n>>), ok end,
    ReceiveFun = fun() -> gen_server2:cast(Pid, client_timeout) end,
    Heartbeat = rabbit_heartbeat:start(SupPid, Sock, SendTimeout,
                                       SendFun, ReceiveTimeout, ReceiveFun),
    {noreply, State#state{heartbeat = Heartbeat}};



%%----------------------------------------------------------------------------
handle_info({'EXIT', From, Reason}, State) ->
  ProcState = processor_state(State),
  case rabbit_stomp_processor:handle_exit(From, Reason, ProcState) of
    {stop, Reason, NewProcState} ->
        {stop, Reason, processor_state(NewProcState, State)};
    unknown_exit ->
        {stop, {connection_died, Reason}, State}
  end;
%%----------------------------------------------------------------------------

handle_info(emit_stats, State) ->
    {noreply, emit_stats(State)};

handle_info(Info, State) ->
    {stop, {odd_info, Info}, State}.



handle_call(Request, _From, State) ->
    {stop, {odd_request, Request}, State}.

terminate(_Reason, State = #state{conn = Conn, proc_state = ProcessorState}) ->
    maybe_emit_stats(State),
    ok = file_handle_cache:release(),
    rabbit_stomp_processor:flush_and_die(ProcessorState),
    Conn:close(1000, "STOMP died"),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------


process_received_bytes(Bytes, ProcessorState, ParseState, ConnPid) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {ok, Frame, Rest} ->
            case rabbit_stomp_processor:process_frame(Frame, ProcessorState) of
                {ok, NewProcState, ConnPid1} ->
                    ParseState1 = rabbit_stomp_frame:initial_state(),
                    process_received_bytes(Rest, NewProcState, ParseState1, ConnPid1);
                {stop, Reason, NewProcState} ->
                    {stop, Reason, NewProcState, ParseState}
            end;
        {more, ParseState1} ->
            {ok, ProcessorState, ParseState1, ConnPid}
    end.

processor_state(#state{ proc_state = ProcState }) -> ProcState.
processor_state(ProcState, #state{} = State) ->
  State#state{ proc_state = ProcState}.

%%----------------------------------------------------------------------------

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
emit_stats(State=#state{conn=Conn, connection=ConnPid}) ->
    Info = Conn:info(),
    Sock = proplists:get_value(socket, Info),
    SockInfos = case rabbit_net:getstat(Sock,
            [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]) of
        {ok,    SI} -> SI;
        {error,  _} -> []
    end,
    Infos = [{pid, ConnPid}|SockInfos],
    rabbit_core_metrics:connection_stats(ConnPid, Infos),
    rabbit_event:notify(connection_stats, Infos),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    State1.
