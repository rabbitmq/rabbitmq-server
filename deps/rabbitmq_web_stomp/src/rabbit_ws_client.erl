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
%% Copyright (c) 2012-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_ws_client).
-behaviour(gen_server).

-include_lib("rabbitmq_stomp/include/rabbit_stomp.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/1]).
-export([sockjs_msg/2, sockjs_closed/1]).

-export([init/1, handle_call/3, handle_info/2, terminate/2,
         code_change/3, handle_cast/2]).

-record(state, {conn, proc_state, parse_state}).

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
    {ok, ProcessorState} = init_processor_state(SupPid, Conn, Heartbeat),
    {ok, #state{conn        = Conn,
                proc_state  = ProcessorState,
                parse_state = rabbit_stomp_frame:initial_state()}}.

init_processor_state(SupPid, Conn, Heartbeat) ->
    StompConfig = #stomp_configuration{implicit_connect = false},

    SendFun = fun (_Sync, Data) ->
                      Conn:send(Data),
                      ok
              end,
    Pid = self(),
    ReceiveFun = fun() -> gen_server:cast(Pid, client_timeout) end,
    Info = Conn:info(),
    Sock = proplists:get_value(socket, Info),
    {PeerAddr, PeerPort} = proplists:get_value(peername, Info),
    {SockAddr, SockPort} = proplists:get_value(sockname, Info),
    Name = rabbit_misc:format("~s:~b -> ~s:~b",
                              [rabbit_misc:ntoa(PeerAddr), PeerPort,
                               rabbit_misc:ntoa(SockAddr), SockPort]),
    AdapterInfo = #amqp_adapter_info{protocol        = {'Web STOMP', 0},
                                     host            = SockAddr,
                                     port            = SockPort,
                                     peer_host       = PeerAddr,
                                     peer_port       = PeerPort,
                                     name            = list_to_binary(Name),
                                     additional_info = [{ssl, rabbit_net:is_ssl(Sock)}]},

    StartHeartbeatFun = case Heartbeat of
        heartbeat ->
            fun (SendTimeout, SendFin, ReceiveTimeout, ReceiveFin) ->
                    rabbit_heartbeat:start(SupPid, Sock, SendTimeout,
                                           SendFin, ReceiveTimeout, ReceiveFin)
            end;
        no_heartbeat ->
            undefined
    end,

    ProcessorState = rabbit_stomp_processor:initial_state(
        StompConfig, 
        {SendFun, ReceiveFun, AdapterInfo, StartHeartbeatFun, none, PeerAddr}),
    {ok, ProcessorState}.

handle_cast({sockjs_msg, Data}, State = #state{proc_state  = ProcessorState,
                                               parse_state = ParseState}) ->
    case process_received_bytes(Data, ProcessorState, ParseState) of
        {ok, NewProcState, ParseState1} ->
            {noreply, State#state{
                            parse_state = ParseState1,
                            proc_state  = NewProcState}};
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
      {ok, NewProcState} ->
        {noreply, processor_state(NewProcState, State)};
      {stop, Reason, NewProcState} ->
        {stop, Reason, processor_state(NewProcState, State)}
    end;

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


handle_info(Info, State) ->
    {stop, {odd_info, Info}, State}.



handle_call(Request, _From, State) ->
    {stop, {odd_request, Request}, State}.

terminate(_Reason, #state{conn = Conn, proc_state = ProcessorState}) ->
    ok = file_handle_cache:release(),
    rabbit_stomp_processor:flush_and_die(ProcessorState),
    Conn:close(1000, "STOMP died"),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------


process_received_bytes(Bytes, ProcessorState, ParseState) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {ok, Frame, Rest} ->
            case rabbit_stomp_processor:process_frame(Frame, ProcessorState) of
                {ok, NewProcState} ->
                    ParseState1 = rabbit_stomp_frame:initial_state(),
                    process_received_bytes(Rest, NewProcState, ParseState1);
                {stop, Reason, NewProcState} ->
                    {stop, Reason, NewProcState, ParseState}
            end;
        {more, ParseState1} ->
            {ok, ProcessorState, ParseState1}
    end.

processor_state(#state{ proc_state = ProcState }) -> ProcState.
processor_state(ProcState, #state{} = State) -> 
  State#state{ proc_state = ProcState}.

