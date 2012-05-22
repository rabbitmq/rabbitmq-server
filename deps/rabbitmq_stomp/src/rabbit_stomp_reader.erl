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

-module(rabbit_stomp_reader).

-export([start_link/2]).
-export([init/2]).
-export([conserve_resources/2]).

-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(reader_state, {socket, parse_state, processor, state,
                       conserve_resources}).

%%----------------------------------------------------------------------------

start_link(SupPid, Configuration) ->
        {ok, proc_lib:spawn_link(?MODULE, init, [SupPid, Configuration])}.

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

init(SupPid, Configuration) ->
    receive
        {go, Sock0, SockTransform} ->
            {ok, Sock} = SockTransform(Sock0),
            {ok, ProcessorPid} = start_processor(SupPid, Configuration, Sock),
            {ok, ConnStr} = rabbit_net:connection_string(Sock, inbound),
            log(info, "accepting STOMP connection ~p (~s)~n",
                [self(), ConnStr]),

            ParseState = rabbit_stomp_frame:initial_state(),
            try
                mainloop(
                  control_throttle(
                    register_resource_alarm(
                      #reader_state{socket             = Sock,
                                    parse_state        = ParseState,
                                    processor          = ProcessorPid,
                                    state              = running,
                                    conserve_resources = false})), 0),
                log(info, "closing STOMP connection ~p (~s)~n",
                    [self(), ConnStr])
            catch
                Ex -> log(error, "closing STOMP connection ~p (~s):~n~p~n",
                          [self(), ConnStr, Ex])
            after
                rabbit_stomp_processor:flush_and_die(ProcessorPid)
            end,

            done
    end.

mainloop(State = #reader_state{socket = Sock}, ByteCount) ->
    run_socket(State, ByteCount),
    receive
        {inet_async, Sock, _Ref, {ok, Data}} ->
            process_received_bytes(Data, State);
        {inet_async, _Sock, _Ref, {error, closed}} ->
            ok;
        {inet_async, _Sock, _Ref, {error, Reason}} ->
            throw({inet_error, Reason});
        {conserve_resources, Conserve} ->
            mainloop(
              control_throttle(
                State#reader_state{conserve_resources = Conserve}), ByteCount);
        {bump_credit, Msg} ->
            credit_flow:handle_bump_msg(Msg),
            mainloop(control_throttle(State), ByteCount)
    end.

process_received_bytes([], State) ->
    mainloop(State, 0);
process_received_bytes(Bytes,
                       State = #reader_state{
                         processor   = Processor,
                         parse_state = ParseState,
                         state       = S}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1, Length} ->
            mainloop(State#reader_state{parse_state = ParseState1}, Length);
        {ok, Frame, Rest} ->
            rabbit_stomp_processor:process_frame(Processor, Frame),
            PS = rabbit_stomp_frame:initial_state(),
            process_received_bytes(Rest,
                                   control_throttle(
                                     State#reader_state{
                                       parse_state = PS,
                                       state       = next_state(S, Frame)}))
    end.

conserve_resources(Pid, Conserve) ->
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

run_socket(#reader_state{state = blocked}, _ByteCount) ->
    ok;
run_socket(#reader_state{socket = Sock}, ByteCount) ->
    rabbit_net:async_recv(Sock, ByteCount, infinity),
    ok.

%%----------------------------------------------------------------------------

start_processor(SupPid, Configuration, Sock) ->
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
                SHF = rabbit_heartbeat:start_heartbeat_fun(SupPid),
                SHF(Sock, SendTimeout, SendFin, ReceiveTimeout, ReceiveFun)
        end,

    rabbit_stomp_client_sup:start_processor(
      SupPid, [SendFun, adapter_info(Sock), StartHeartbeatFun,
               ssl_login_name(Sock, Configuration), Configuration]).


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
    #adapter_info{protocol        = {'STOMP', 0},
                  name            = list_to_binary(Name),
                  address         = Addr,
                  port            = Port,
                  peer_address    = PeerAddr,
                  peer_port       = PeerPort,
                  additional_info = maybe_ssl_info(Sock)}.

maybe_ssl_info(Sock) ->
    case rabbit_net:is_ssl(Sock) of
        true  -> [{ssl, true}] ++ ssl_info(Sock) ++ ssl_cert_info(Sock);
        false -> [{ssl, false}]
    end.

ssl_info(Sock) ->
    {Protocol, KeyExchange, Cipher, Hash} =
        case rabbit_net:ssl_info(Sock) of
            {ok, {P, {K, C, H}}}    -> {P, K, C, H};
            {ok, {P, {K, C, H, _}}} -> {P, K, C, H};
            _                       -> {unknown, unknown, unknown, unknown}
        end,
    [{ssl_protocol,       Protocol},
     {ssl_key_exchange,   KeyExchange},
     {ssl_cipher,         Cipher},
     {ssl_hash,           Hash}].

ssl_cert_info(Sock) ->
    case rabbit_net:peercert(Sock) of
        {ok, Cert} ->
            [{peer_cert_issuer,   list_to_binary(
                                    rabbit_ssl:peer_cert_issuer(Cert))},
             {peer_cert_subject,  list_to_binary(
                                    rabbit_ssl:peer_cert_subject(Cert))},
             {peer_cert_validity, list_to_binary(
                                    rabbit_ssl:peer_cert_validity(Cert))}];
        _ ->
            []
    end.

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
