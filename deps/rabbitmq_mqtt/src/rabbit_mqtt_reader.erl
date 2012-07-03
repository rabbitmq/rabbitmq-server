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

-export([start_link/2]).
-export([init/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("include/rabbit_mqtt_frame.hrl").

-record(reader_state, {socket, parse_state, processor, state,
                       conserve_resources, recv_outstanding}).

%%----------------------------------------------------------------------------

start_link(SupPid, Configuration) ->
        {ok, proc_lib:spawn_link(?MODULE, init, [SupPid, Configuration])}.

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

init(SupPid, Configuration) ->
    receive
        {go, Sock} ->
            {ok, ProcessorPid} = start_processor(SupPid, Configuration, Sock),
            {ok, ConnStr} = rabbit_net:connection_string(Sock, inbound),

            log(info, "accepting MQTT connection ~p (~s)~n",
                [self(), ConnStr]),

            ParseState = rabbit_mqtt_frame:initial_state(),
            try
                rabbit_misc:throw_on_error(
                  inet_error, fun () -> rabbit_net:tune_buffer_size(Sock) end),
                mainloop(
                  control_throttle(
                    register_resource_alarm(
                      #reader_state{socket             = Sock,
                                    parse_state        = ParseState,
                                    processor          = ProcessorPid,
                                    state              = running,
                                    conserve_resources = false,
                                    recv_outstanding   = false}))),
                log(info, "closing MQTT connection ~p (~s)~n",
                    [self(), ConnStr])
            catch
                _:Ex -> log(error, "closing MQTT connection ~p (~s):~n~p~n",
                          [self(), ConnStr, Ex])
            after
                rabbit_mqtt_processor:flush_and_die(ProcessorPid)
            end,

            done
    end.

mainloop(State0 = #reader_state{socket = Sock}) ->
    State = run_socket(State0),
    receive
        {inet_async, Sock, _Ref, {ok, Data}} ->
            process_received_bytes(
              Data, State#reader_state{recv_outstanding = false});
        {inet_async, _Sock, _Ref, {error, closed}} ->
            ok;
        {inet_async, _Sock, _Ref, {error, Reason}} ->
            throw({inet_error, Reason});
        {conserve_resources, Conserve} ->
            mainloop(control_throttle(
                       State#reader_state{conserve_resources = Conserve}));
        {bump_credit, Msg} ->
            credit_flow:handle_bump_msg(Msg),
            mainloop(control_throttle(State))
    end.

process_received_bytes([], State) ->
    mainloop(State);
process_received_bytes(Bytes,
                       State = #reader_state{
                         processor   = Processor,
                         parse_state = ParseState,
                         state       = S}) ->
    case rabbit_mqtt_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            mainloop(State#reader_state{parse_state = ParseState1});
        {ok, Frame, Rest} ->
            rabbit_mqtt_processor:process_frame(Processor, Frame),
            PS = rabbit_mqtt_frame:initial_state(),
            process_received_bytes(Rest,
                                   control_throttle(
                                     State#reader_state{
                                       parse_state = PS,
                                       state       = next_state(S, Frame)}));
        {error, Error} ->
            rabbit_log:error("MQTT parse error ~p~n", [error]),
            throw(Error)
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

next_state(blocking, {frame, publish}) ->
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

    rabbit_mqtt_client_sup:start_processor(
      SupPid, [SendFun, adapter_info(Sock), Configuration]).


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
    #adapter_info{protocol        = {'MQTT', {?MQTT_PROTO_MAJOR, ?MQTT_PROTO_MINOR}},
                  name            = list_to_binary(Name),
                  address         = Addr,
                  port            = Port,
                  peer_address    = PeerAddr,
                  peer_port       = PeerPort}.
