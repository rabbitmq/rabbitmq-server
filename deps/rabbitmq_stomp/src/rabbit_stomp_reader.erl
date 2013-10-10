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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_stomp_reader).

-export([start_link/3]).
-export([init/3]).
-export([conserve_resources/3]).

-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(reader_state, {socket, parse_state, processor, state,
                       conserve_resources, recv_outstanding}).

%%----------------------------------------------------------------------------

start_link(SupPid, ProcessorPid, Configuration) ->
        {ok, proc_lib:spawn_link(?MODULE, init,
                                 [SupPid, ProcessorPid, Configuration])}.

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

init(SupPid, ProcessorPid, Configuration) ->
    Reply = go(SupPid, ProcessorPid, Configuration),
    rabbit_stomp_processor:flush_and_die(ProcessorPid),
    Reply.

go(SupPid, ProcessorPid, Configuration) ->
    receive
        {go, Sock0, SockTransform} ->
            {ok, Sock} = SockTransform(Sock0),
            case rabbit_net:connection_string(Sock, inbound) of
                {ok, ConnStr} ->
                    ProcInitArgs = processor_args(SupPid, Configuration, Sock),
                    rabbit_stomp_processor:init_arg(ProcessorPid, ProcInitArgs),
                    log(info, "accepting STOMP connection ~p (~s)~n",
                        [self(), ConnStr]),

                    ParseState = rabbit_stomp_frame:initial_state(),
                    try
                        mainloop(
                          register_resource_alarm(
                            #reader_state{socket             = Sock,
                                          parse_state        = ParseState,
                                          processor          = ProcessorPid,
                                          state              = running,
                                          conserve_resources = false,
                                          recv_outstanding   = false})),
                        log(info, "closing STOMP connection ~p (~s)~n",
                            [self(), ConnStr])
                    catch
                        _:Ex -> log(error, "closing STOMP connection "
                                    "~p (~s):~n~p~n", [self(), ConnStr, Ex])
                    end,
                    done;
                {error, enotconn} ->
                    rabbit_net:fast_close(Sock),
                    done;
                {error, Reason} ->
                    log(warning, "STOMP network error while starting up: ~p~n",
                        [Reason]),
                    rabbit_net:fast_close(Sock)
            end
    end.

mainloop(State0 = #reader_state{socket = Sock}) ->
    State = run_socket(control_throttle(State0)),
    receive
        {inet_async, Sock, _Ref, {ok, Data}} ->
            mainloop(process_received_bytes(
                       Data, State#reader_state{recv_outstanding = false}));
        {inet_async, _Sock, _Ref, {error, closed}} ->
            ok;
        {inet_async, _Sock, _Ref, {error, Reason}} ->
            throw({inet_error, Reason});
        {conserve_resources, Conserve} ->
            mainloop(State#reader_state{conserve_resources = Conserve});
        {bump_credit, Msg} ->
            credit_flow:handle_bump_msg(Msg),
            mainloop(State)
    end.

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

register_resource_alarm(State) ->
    rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}), State.

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
    [SendFun, adapter_info(Sock), StartHeartbeatFun,
     ssl_login_name(Sock, Configuration)].

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
