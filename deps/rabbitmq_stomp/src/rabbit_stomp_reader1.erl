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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_stomp_reader1).
-behaviour(gen_server2).

-export([start_link/5]).
-export([conserve_resources/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(reader_state, {socket, conn_name, parse_state, processor, state,
                       conserve_resources, recv_outstanding,
                       parent}).

%%----------------------------------------------------------------------------

start_link(SupHelperPid, ProcessorPid, Ref, Sock, Configuration) ->
    Pid = proc_lib:spawn_link(?MODULE, init,
                              [[SupHelperPid, ProcessorPid, Ref, Sock, Configuration]]),

    %% In the event that somebody floods us with connections, the
    %% reader processes can spew log events at error_logger faster
    %% than it can keep up, causing its mailbox to grow unbounded
    %% until we eat all the memory available and crash. So here is a
    %% meaningless synchronous call to the underlying gen_event
    %% mechanism. When it returns the mailbox is drained, and we
    %% return to our caller to accept more connections.
    gen_event:which_handlers(error_logger),

    {ok, Pid}.

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

init([SupHelperPid, ProcessorPid, Ref, Sock, Configuration]) ->
    process_flag(trap_exit, true),
    rabbit_net:accept_ack(Ref, Sock),
    
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            %from go/4
            DebugOpts = sys:debug_options([]),
            ProcInitArgs = processor_args(SupHelperPid,
                                          Configuration,
                                          Sock),
            rabbit_stomp_processor:init_arg(ProcessorPid,
                                            ProcInitArgs),
            log(info, "accepting STOMP connection ~p (~s)~n",
                [self(), ConnStr]),

            ParseState = rabbit_stomp_frame:initial_state(),
            register_resource_alarm(),
            gen_server2:enter_loop(?MODULE, [],
              run_socket(control_throttle(
                #reader_state{socket             = Sock,
                              conn_name          = ConnStr,
                              parse_state        = ParseState,
                              processor          = ProcessorPid,
                              state              = running,
                              conserve_resources = false,
                              recv_outstanding   = false})),
              {backoff, 1000, 1000, 10000});
        {network_error, Reason} ->
            rabbit_net:fast_close(Sock),
            terminate({shutdown, Reason}, undefined);
        {error, enotconn} ->
            rabbit_net:fast_close(Sock),
            terminate(shutdown, undefined);
        {error, Reason} ->
            rabbit_net:fast_close(Sock),
            terminate({network_error, Reason}, undefined)
    end.


handle_call(Msg, From, State) ->
    {stop, {stomp_unexpected_call, Msg, From}, State}.

handle_cast(Msg, State) ->
    {stop, {stomp_unexpected_cast, Msg}, State}.


handle_info({inet_async, Sock, _Ref, {ok, Data}}, State) ->
    NewState = process_received_bytes(Data, 
        State#reader_state{recv_outstanding = false}),
    {noreply, NewState};
handle_info({inet_async, _Sock, _Ref, {error, closed}}, State) ->
    {noreply, State};
handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    {stop, {inet_error, Reason}, State};
handle_info({inet_reply, _Sock, {error, closed}}, State) ->
    ok;
handle_info({conserve_resources, Conserve}, State) ->
    NewState = State#reader_state{conserve_resources = Conserve},
    {noreply, NewState};
handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    {noreply, State};
handle_info({'EXIT', _From, Reason}, State) ->
    {stop, {connection_died, Reason}, State}.


process_received_bytes([], State) ->
    State;
process_received_bytes(Bytes,
                       State = #reader_state{
                         processor   = Processor,
                         parse_state = ParseState,
                         state       = S}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            State#reader_state{parse_state = ParseState1};
        {ok, Frame, Rest} ->
            rabbit_stomp_processor:process_frame(Processor, Frame),
            PS = rabbit_stomp_frame:initial_state(),
            process_received_bytes(Rest, State#reader_state{
                                           parse_state = PS,
                                           state       = next_state(S, Frame)})
    end.

conserve_resources(Pid, _Source, Conserve) ->
    Pid ! {conserve_resources, Conserve},
    ok.

register_resource_alarm() ->
    rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}).


control_throttle(State = #reader_state{state              = CS,
                                       conserve_resources = Mem}) ->
    case {CS, Mem orelse credit_flow:blocked()} of
        {running,   true} -> State#reader_state{state = blocking};
        {blocking, false} -> State#reader_state{state = running};
        {blocked,  false} -> State#reader_state{state = running};
        {_,            _} -> State
    end.

next_state(blocking, #stomp_frame{command = "SEND"}) ->
    blocked;
next_state(S, _) ->
    S.

run_socket(State = #reader_state{state = blocked}) ->
    State;
run_socket(State = #reader_state{recv_outstanding = true}) ->
    State;
run_socket(State = #reader_state{socket = Sock}) ->
    rabbit_net:async_recv(Sock, 0, infinity),
    State#reader_state{recv_outstanding = true}.


terminate(Reason, State = #reader_state{ processor = ProcessorPid }) ->
  log_reason(Reason, State),
  rabbit_stomp_processor:flush_and_die(ProcessorPid),
  ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


log_reason({network_error, {ssl_upgrade_error, closed}, ConnStr}, _State) ->
    log(error, "STOMP detected TLS upgrade error on ~s: connection closed~n",
       [ConnStr]);

log_reason({network_error,
           {ssl_upgrade_error,
            {tls_alert, "handshake failure"}}, ConnStr}, _State) ->
    log(error, "STOMP detected TLS upgrade error on ~s: handshake failure~n",
       [ConnStr]);

log_reason({network_error,
           {ssl_upgrade_error,
            {tls_alert, "unknown ca"}}, ConnStr}, _State) ->
    log(error, "STOMP detected TLS certificate verification error on ~s: alert 'unknown CA'~n",
       [ConnStr]);

log_reason({network_error,
           {ssl_upgrade_error,
            {tls_alert, Alert}}, ConnStr}, _State) ->
    log(error, "STOMP detected TLS upgrade error on ~s: alert ~s~n",
       [ConnStr, Alert]);

log_reason({network_error, {ssl_upgrade_error, Reason}, ConnStr}, _State) ->
    log(error, "STOMP detected TLS upgrade error on ~s: ~p~n",
        [ConnStr, Reason]);

log_reason({network_error, Reason, ConnStr}, _State) ->
    log(error, "STOMP detected network error on ~s: ~p~n",
        [ConnStr, Reason]);

log_reason({network_error, Reason}, _State) ->
    log(error, "STOMP detected network error: ~p~n", [Reason]);

log_reason(normal, #reader_state{ conn_name  = ConnName}) ->
    log(info, "closing STOMP connection ~p (~s)~n", [self(), ConnName]).


%%----------------------------------------------------------------------------

processor_args(SupPid, Configuration, Sock) ->
    SendFun = fun (sync, IoData) ->
                      %% no messages emitted
                      catch rabbit_net:send(Sock, IoData);
                  (async, IoData) ->
                      %% {inet_reply, _, _} will appear soon
                      %% We ignore certain errors here, as we will be
                      %% receiving an asynchronous notification of the
                      %% same (or a related) fault shortly anyway. See
                      %% bug 21365.
                      catch rabbit_net:port_command(Sock, IoData)
              end,

    StartHeartbeatFun =
        fun (SendTimeout, SendFin, ReceiveTimeout, ReceiveFun) ->
                rabbit_heartbeat:start(SupPid, Sock, SendTimeout,
                                       SendFin, ReceiveTimeout, ReceiveFun)
        end,
    {ok, {PeerAddr, _PeerPort}} = rabbit_net:sockname(Sock),
    [SendFun, adapter_info(Sock), StartHeartbeatFun,
     ssl_login_name(Sock, Configuration), PeerAddr].

adapter_info(Sock) ->
    amqp_connection:socket_adapter_info(Sock, {'STOMP', 0}).

ssl_login_name(_Sock, #stomp_configuration{ssl_cert_login = false}) ->
    none;
ssl_login_name(Sock, #stomp_configuration{ssl_cert_login = true}) ->
    case rabbit_net:peercert(Sock) of
        {ok, C}              -> case rabbit_ssl:peer_cert_auth_name(C) of
                                    unsafe    -> none;
                                    not_found -> none;
                                    Name      -> Name
                                end;
        {error, no_peercert} -> none;
        nossl                -> none
    end.

%%----------------------------------------------------------------------------

