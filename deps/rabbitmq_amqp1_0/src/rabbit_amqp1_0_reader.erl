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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_reader).

%% Begin 1-0
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("rabbit_amqp1_0.hrl").
%% End 1-0

-export([start_link/3, info_keys/0, info/1, info/2, shutdown/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/4, mainloop/2]).

-export([conserve_memory/2, server_properties/1]).

-export([process_channel_frame/5]). %% used by erlang-client

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 1).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).
-define(SILENT_CLOSE_DELAY, 3).

%%--------------------------------------------------------------------------

-record(v1, {parent, sock, connection, callback, recv_len, pending_recv,
             connection_state, queue_collector, heartbeater, stats_timer,
             channel_sup_sup_pid, start_heartbeat_fun, buf, buf_len,
             auth_mechanism, auth_state}).

-define(STATISTICS_KEYS, [pid, recv_oct, recv_cnt, send_oct, send_cnt,
                          send_pend, state, channels]).

-define(CREATION_EVENT_KEYS, [pid, address, port, peer_address, peer_port, ssl,
                              peer_cert_subject, peer_cert_issuer,
                              peer_cert_validity, auth_mechanism,
                              ssl_protocol, ssl_key_exchange,
                              ssl_cipher, ssl_hash,
                              protocol, user, vhost, timeout, frame_max,
                              client_properties]).

-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

-define(IS_RUNNING(State),
        (State#v1.connection_state =:= running orelse
         State#v1.connection_state =:= blocking orelse
         State#v1.connection_state =:= blocked)).

%%--------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/3 :: (pid(), pid(), rabbit_heartbeat:start_heartbeat_fun()) ->
                           rabbit_types:ok(pid())).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (pid()) -> rabbit_types:infos()).
-spec(info/2 :: (pid(), rabbit_types:info_keys()) -> rabbit_types:infos()).
-spec(shutdown/2 :: (pid(), string()) -> 'ok').
-spec(conserve_memory/2 :: (pid(), boolean()) -> 'ok').
-spec(server_properties/1 :: (rabbit_types:protocol()) ->
                                  rabbit_framing:amqp_table()).

%% These specs only exists to add no_return() to keep dialyzer happy
-spec(init/4 :: (pid(), pid(), pid(), rabbit_heartbeat:start_heartbeat_fun())
                -> no_return()).
-spec(start_connection/7 ::
        (pid(), pid(), pid(), rabbit_heartbeat:start_heartbeat_fun(), any(),
         rabbit_net:socket(),
         fun ((rabbit_net:socket()) ->
                     rabbit_types:ok_or_error2(
                       rabbit_net:socket(), any()))) -> no_return()).

-endif.

%%--------------------------------------------------------------------------

start_link(ChannelSupSupPid, Collector, StartHeartbeatFun) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [self(), ChannelSupSupPid,
                                             Collector, StartHeartbeatFun])}.

shutdown(Pid, Explanation) ->
    gen_server:call(Pid, {shutdown, Explanation}, infinity).

init(Parent, ChannelSupSupPid, Collector, StartHeartbeatFun) ->
    Deb = sys:debug_options([]),
    receive
        {go, Sock, SockTransform} ->
            start_connection(
              Parent, ChannelSupSupPid, Collector, StartHeartbeatFun, Deb, Sock,
              SockTransform)
    end.

system_continue(Parent, Deb, State) ->
    ?MODULE:mainloop(Deb, State#v1{parent = Parent}).

system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

info_keys() -> ?INFO_KEYS.

info(Pid) ->
    gen_server:call(Pid, info, infinity).

info(Pid, Items) ->
    case gen_server:call(Pid, {info, Items}, infinity) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

conserve_memory(Pid, Conserve) ->
    Pid ! {conserve_memory, Conserve},
    ok.

server_properties(Protocol) ->
    {ok, Product} = application:get_key(rabbit, id),
    {ok, Version} = application:get_key(rabbit, vsn),

    %% Get any configuration-specified server properties
    {ok, RawConfigServerProps} = application:get_env(rabbit,
                                                     server_properties),

    %% Normalize the simplifed (2-tuple) and unsimplified (3-tuple) forms
    %% from the config and merge them with the generated built-in properties
    NormalizedConfigServerProps =
        [{<<"capabilities">>, table, server_capabilities(Protocol)} |
         [case X of
              {KeyAtom, Value} -> {list_to_binary(atom_to_list(KeyAtom)),
                                   longstr,
                                   list_to_binary(Value)};
              {BinKey, Type, Value} -> {BinKey, Type, Value}
          end || X <- RawConfigServerProps ++
                     [{product,     Product},
                      {version,     Version},
                      {platform,    "Erlang/OTP"},
                      {copyright,   ?COPYRIGHT_MESSAGE},
                      {information, ?INFORMATION_MESSAGE}]]],

    %% Filter duplicated properties in favour of config file provided values
    lists:usort(fun ({K1,_,_}, {K2,_,_}) -> K1 =< K2 end,
                NormalizedConfigServerProps).

server_capabilities(rabbit_framing_amqp_0_9_1) ->
    [{<<"publisher_confirms">>,         bool, true},
     {<<"exchange_exchange_bindings">>, bool, true},
     {<<"basic.nack">>,                 bool, true},
     {<<"consumer_cancel_notify">>,     bool, true}];
server_capabilities(_) ->
    [].

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

socket_op(Sock, Fun) ->
    case Fun(Sock) of
        {ok, Res}       -> Res;
        {error, Reason} -> rabbit_log:error("error on TCP connection ~p:~p~n",
                                            [self(), Reason]),
                           rabbit_log:info("closing TCP connection ~p~n",
                                           [self()]),
                           exit(normal)
    end.

start_connection(Parent, ChannelSupSupPid, Collector, StartHeartbeatFun, Deb,
                 Sock, SockTransform) ->
    process_flag(trap_exit, true),
    {PeerAddress, PeerPort} = socket_op(Sock, fun rabbit_net:peername/1),
    PeerAddressS = rabbit_misc:ntoab(PeerAddress),
    rabbit_log:info("starting TCP connection ~p from ~s:~p~n",
                    [self(), PeerAddressS, PeerPort]),
    ClientSock = socket_op(Sock, SockTransform),
    erlang:send_after(?HANDSHAKE_TIMEOUT * 1000, self(),
                      handshake_timeout),
    try
        recvloop(Deb, switch_callback(
                        #v1{parent              = Parent,
                            sock                = ClientSock,
                            connection          = #connection{
                              protocol           = none,
                              user               = none,
                              timeout_sec        = ?HANDSHAKE_TIMEOUT,
                              frame_max          = ?FRAME_MIN_SIZE,
                              vhost              = none,
                              client_properties  = none,
                              capabilities       = []},
                            callback            = uninitialized_callback,
                            recv_len            = 0,
                            pending_recv        = false,
                            connection_state    = pre_init,
                            queue_collector     = Collector,
                            heartbeater         = none,
                            stats_timer         =
                                rabbit_event:init_stats_timer(),
                            channel_sup_sup_pid = ChannelSupSupPid,
                            start_heartbeat_fun = StartHeartbeatFun,
                            buf                 = [],
                            buf_len             = 0,
                            auth_mechanism      = none,
                            auth_state          = none
                           },
                        handshake, 8))
    catch
        Ex -> (if Ex == connection_closed_abruptly ->
                       fun rabbit_log:warning/2;
                  true ->
                       fun rabbit_log:error/2
               end)("exception on TCP connection ~p from ~s:~p~n~p~n",
                    [self(), PeerAddressS, PeerPort, Ex])
    after
        rabbit_log:info("closing TCP connection ~p from ~s:~p~n",
                        [self(), PeerAddressS, PeerPort]),
        %% We don't close the socket explicitly. The reader is the
        %% controlling process and hence its termination will close
        %% the socket. Furthermore, gen_tcp:close/1 waits for pending
        %% output to be sent, which results in unnecessary delays.
        %%
        %% gen_tcp:close(ClientSock),
        rabbit_event:notify(connection_closed, [{pid, self()}])
    end,
    done.

recvloop(Deb, State = #v1{pending_recv = true}) ->
    mainloop(Deb, State);
recvloop(Deb, State = #v1{connection_state = blocked}) ->
    mainloop(Deb, State);
recvloop(Deb, State = #v1{sock = Sock, recv_len = RecvLen, buf_len = BufLen})
  when BufLen < RecvLen ->
    ok = rabbit_net:setopts(Sock, [{active, once}]),
    mainloop(Deb, State#v1{pending_recv = true});
recvloop(Deb, State = #v1{recv_len = RecvLen, buf = Buf, buf_len = BufLen}) ->
    {Data, Rest} = split_binary(case Buf of
                                    [B] -> B;
                                    _   -> list_to_binary(lists:reverse(Buf))
                                end, RecvLen),
    recvloop(Deb, handle_input(State#v1.callback, Data,
                               State#v1{buf = [Rest],
                                        buf_len = BufLen - RecvLen})).

mainloop(Deb, State = #v1{sock = Sock, buf = Buf, buf_len = BufLen}) ->
    case rabbit_net:recv(Sock) of
        {data, Data}    -> recvloop(Deb, State#v1{buf = [Data | Buf],
                                                  buf_len = BufLen + size(Data),
                                                  pending_recv = false});
        closed          -> if State#v1.connection_state =:= closed ->
                                   State;
                              true ->
                                   throw(connection_closed_abruptly)
                           end;
        {error, Reason} -> throw({inet_error, Reason});
        {other, Other}  -> handle_other(Other, Deb, State)
    end.

handle_other({conserve_memory, Conserve}, Deb, State) ->
    recvloop(Deb, internal_conserve_memory(Conserve, State));
handle_other({channel_closing, ChPid}, Deb, State) ->
    ok = rabbit_channel:ready_for_close(ChPid),
    channel_cleanup(ChPid),
    mainloop(Deb, State);
handle_other({'EXIT', Parent, Reason}, _Deb, State = #v1{parent = Parent}) ->
    terminate(io_lib:format("broker forced connection closure "
                            "with reason '~w'", [Reason]), State),
    %% this is what we are expected to do according to
    %% http://www.erlang.org/doc/man/sys.html
    %%
    %% If we wanted to be *really* nice we should wait for a while for
    %% clients to close the socket at their end, just as we do in the
    %% ordinary error case. However, since this termination is
    %% initiated by our parent it is probably more important to exit
    %% quickly.
    exit(Reason);
handle_other({channel_exit, _Channel, E = {writer, send_failed, _Error}},
             _Deb, _State) ->
    throw(E);
handle_other({channel_exit, Channel, Reason}, Deb, State) ->
    mainloop(Deb, handle_exception(State, Channel, Reason));
handle_other({'DOWN', _MRef, process, ChPid, Reason}, Deb, State) ->
    mainloop(Deb, handle_dependent_exit(ChPid, Reason, State));
handle_other(terminate_connection, _Deb, State) ->
    State;
handle_other(handshake_timeout, Deb, State)
  when ?IS_RUNNING(State) orelse
       State#v1.connection_state =:= closing orelse
       State#v1.connection_state =:= closed ->
    mainloop(Deb, State);
handle_other(handshake_timeout, _Deb, State) ->
    throw({handshake_timeout, State#v1.callback});
handle_other(timeout, Deb, State = #v1{connection_state = closed}) ->
    mainloop(Deb, State);
handle_other(timeout, _Deb, #v1{connection_state = S}) ->
    throw({timeout, S});
handle_other({'$gen_call', From, {shutdown, Explanation}}, Deb, State) ->
    {ForceTermination, NewState} = terminate(Explanation, State),
    gen_server:reply(From, ok),
    case ForceTermination of
        force  -> ok;
        normal -> mainloop(Deb, NewState)
    end;
handle_other({'$gen_call', From, info}, Deb, State) ->
    gen_server:reply(From, infos(?INFO_KEYS, State)),
    mainloop(Deb, State);
handle_other({'$gen_call', From, {info, Items}}, Deb, State) ->
    gen_server:reply(From, try {ok, infos(Items, State)}
                           catch Error -> {error, Error}
                           end),
    mainloop(Deb, State);
handle_other(emit_stats, Deb, State) ->
    mainloop(Deb, emit_stats(State));
handle_other({system, From, Request}, Deb, State = #v1{parent = Parent}) ->
    sys:handle_system_msg(Request, From, Parent, ?MODULE, Deb, State);
handle_other(Other, _Deb, _State) ->
    %% internal error -> something worth dying for
    exit({unexpected_message, Other}).

switch_callback(State = #v1{connection_state = blocked,
                            heartbeater = Heartbeater}, Callback, Length) ->
    ok = rabbit_heartbeat:pause_monitor(Heartbeater),
    State#v1{callback = Callback, recv_len = Length};
switch_callback(State, Callback, Length) ->
    State#v1{callback = Callback, recv_len = Length}.

terminate(Explanation, State) when ?IS_RUNNING(State) ->
    {normal, send_exception(State, 0,
                            rabbit_misc:amqp_error(
                              connection_forced, Explanation, [], none))};
terminate(_Explanation, State) ->
    {force, State}.

internal_conserve_memory(true,  State = #v1{connection_state = running}) ->
    State#v1{connection_state = blocking};
internal_conserve_memory(false, State = #v1{connection_state = blocking}) ->
    State#v1{connection_state = running};
internal_conserve_memory(false, State = #v1{connection_state = blocked,
                                            heartbeater      = Heartbeater}) ->
    ok = rabbit_heartbeat:resume_monitor(Heartbeater),
    State#v1{connection_state = running};
internal_conserve_memory(_Conserve, State) ->
    State.

close_connection(State = #v1{queue_collector = Collector,
                             connection = #connection{
                               timeout_sec = TimeoutSec}}) ->
    %% The spec says "Exclusive queues may only be accessed by the
    %% current connection, and are deleted when that connection
    %% closes."  This does not strictly imply synchrony, but in
    %% practice it seems to be what people assume.
    rabbit_queue_collector:delete_all(Collector),
    %% We terminate the connection after the specified interval, but
    %% no later than ?CLOSING_TIMEOUT seconds.
    TimeoutMillisec =
        1000 * if TimeoutSec > 0 andalso
                  TimeoutSec < ?CLOSING_TIMEOUT -> TimeoutSec;
                  true -> ?CLOSING_TIMEOUT
               end,
    erlang:send_after(TimeoutMillisec, self(), terminate_connection),
    State#v1{connection_state = closed}.

handle_dependent_exit(ChPid, Reason, State) ->
    case termination_kind(Reason) of
        controlled ->
            channel_cleanup(ChPid),
            maybe_close(State);
        uncontrolled ->
            case channel_cleanup(ChPid) of
                undefined -> exit({abnormal_dependent_exit, ChPid, Reason});
                Channel   -> rabbit_log:error(
                               "connection ~p, channel ~p - error:~n~p~n",
                               [self(), Channel, Reason]),
                             maybe_close(
                               handle_exception(State, Channel, Reason))
            end
    end.

channel_cleanup(ChPid) ->
    case get({ch_pid, ChPid}) of
        undefined       -> undefined;
        {Channel, MRef} -> erase({channel, Channel}),
                           erase({ch_pid, ChPid}),
                           erlang:demonitor(MRef, [flush]),
                           Channel
    end.

all_channels() -> [ChPid || {{ch_pid, ChPid}, _ChannelMRef} <- get()].

terminate_channels() ->
    NChannels =
        length([rabbit_channel:shutdown(ChPid) || ChPid <- all_channels()]),
    if NChannels > 0 ->
            Timeout = 1000 * ?CHANNEL_TERMINATION_TIMEOUT * NChannels,
            TimerRef = erlang:send_after(Timeout, self(), cancel_wait),
            wait_for_channel_termination(NChannels, TimerRef);
       true -> ok
    end.

wait_for_channel_termination(0, TimerRef) ->
    case erlang:cancel_timer(TimerRef) of
        false -> receive
                     cancel_wait -> ok
                 end;
        _     -> ok
    end;

wait_for_channel_termination(N, TimerRef) ->
    receive
        {'DOWN', _MRef, process, ChPid, Reason} ->
            case channel_cleanup(ChPid) of
                undefined ->
                    exit({abnormal_dependent_exit, ChPid, Reason});
                Channel ->
                    case termination_kind(Reason) of
                        controlled ->
                            ok;
                        uncontrolled ->
                            rabbit_log:error(
                              "connection ~p, channel ~p - "
                              "error while terminating:~n~p~n",
                              [self(), Channel, Reason])
                    end,
                    wait_for_channel_termination(N-1, TimerRef)
            end;
        cancel_wait ->
            exit(channel_termination_timeout)
    end.

maybe_close(State = #v1{connection_state = closing,
                        connection = #connection{protocol = Protocol},
                        sock = Sock}) ->
    case all_channels() of
        [] ->
            NewState = close_connection(State),
%% Begin 1-0
            ok = case Protocol of
                     rabbit_amqp1_0_framing ->
                         send_on_channel0(Sock, #'v1_0.close'{}, rabbit_amqp1_0_framing);
                     _ ->
                         send_on_channel0(Sock, #'connection.close_ok'{}, Protocol)
                 end,
%% End 1-0
            NewState;
        _  -> State
    end;
maybe_close(State) ->
    State.

termination_kind(normal) -> controlled;
termination_kind(_)      -> uncontrolled.

handle_frame(Type, 0, Payload,
             State = #v1{connection_state = CS,
                         connection = #connection{protocol = Protocol}})
  when CS =:= closing; CS =:= closed ->
    case rabbit_command_assembler:analyze_frame(Type, Payload, Protocol) of
        {method, MethodName, FieldsBin} ->
            handle_method0(MethodName, FieldsBin, State);
        _Other -> State
    end;
handle_frame(_Type, _Channel, _Payload, State = #v1{connection_state = CS})
  when CS =:= closing; CS =:= closed ->
    State;
handle_frame(Type, 0, Payload,
             State = #v1{connection = #connection{protocol = Protocol}}) ->
    case rabbit_command_assembler:analyze_frame(Type, Payload, Protocol) of
        error     -> throw({unknown_frame, 0, Type, Payload});
        heartbeat -> State;
        {method, MethodName, FieldsBin} ->
            handle_method0(MethodName, FieldsBin, State);
        Other -> throw({unexpected_frame_on_channel0, Other})
    end;
handle_frame(Type, Channel, Payload,
             State = #v1{connection = #connection{protocol = Protocol}}) ->
    case rabbit_command_assembler:analyze_frame(Type, Payload, Protocol) of
        error         -> throw({unknown_frame, Channel, Type, Payload});
        heartbeat     -> throw({unexpected_heartbeat_frame, Channel});
        AnalyzedFrame ->
            case get({channel, Channel}) of
                {ChPid, FramingState} ->
                    NewAState = process_channel_frame(
                                  AnalyzedFrame, self(),
                                  Channel, ChPid, FramingState),
                    put({channel, Channel}, {ChPid, NewAState}),
                    case AnalyzedFrame of
                        {method, 'channel.close_ok', _} ->
                            channel_cleanup(ChPid),
                            State;
                        {method, MethodName, _} ->
                            case (State#v1.connection_state =:= blocking
                                  andalso
                                  Protocol:method_has_content(MethodName)) of
                                true  -> State#v1{connection_state = blocked};
                                false -> State
                            end;
                        _ ->
                            State
                    end;
                undefined ->
                    case ?IS_RUNNING(State) of
                        true  -> send_to_new_channel(
                                   Channel, AnalyzedFrame, State);
                        false -> throw({channel_frame_while_starting,
                                        Channel, State#v1.connection_state,
                                        AnalyzedFrame})
                    end
            end
    end.

%% Begin 1-0

%% ----------------------------------------
%% AMQP 1.0 frame handlers

is_connection_frame(#'v1_0.open'{})  -> true;
is_connection_frame(#'v1_0.close'{}) -> true;
is_connection_frame(_)               -> false.

%% FIXME Handle depending on connection state
%% TODO It'd be nice to only decode up to the descriptor

%% Nothing specifies that connection methods have to be on a
%% particular channel.
handle_1_0_frame(_Mode, _Channel, Payload,
                 State = #v1{ connection_state = CS}) when
      CS =:= closing; CS =:= closed ->
    Sections = parse_1_0_frame(Payload),
    case is_connection_frame(Sections) of
        true  -> handle_1_0_connection_frame(Sections, State);
        false -> State
    end;
handle_1_0_frame(Mode, Channel, Payload, State) ->
    Sections = parse_1_0_frame(Payload),
    case {Mode, is_connection_frame(Sections)} of
        {amqp, true}  -> handle_1_0_connection_frame(Sections, State);
        {amqp, false} -> handle_1_0_session_frame(Channel, Sections, State);
        {sasl, _}     -> handle_1_0_sasl_frame(Sections, State)
    end.

parse_1_0_frame(Payload) ->
    Sections = case [rabbit_amqp1_0_framing:decode(Parsed) ||
                        Parsed <- rabbit_amqp1_0_binary_parser:parse(Payload)] of
                   [Value] -> Value;
                   List    -> List
               end,
    ?DEBUG("1.0 frame(s) decoded: ~p~n", [Sections]),
    Sections.

handle_1_0_connection_frame(#'v1_0.open'{ max_frame_size = ClientFrameMax,
                                          hostname = _Hostname,
                                          properties = Props },
                            State = #v1{
                              start_heartbeat_fun = SHF,
                              stats_timer = StatsTimer,
                              connection_state = starting,
                              connection = Connection,
                              sock = Sock}) ->
    Interval = undefined, %% TODO does 1-0 really no longer have heartbeating?
    %% TODO channel_max?
    ClientProps = case Props of
                      undefined -> [];
                      {map, Ps} -> Ps
                  end,
    ClientHeartbeat = case Interval of
                          undefined -> 0;
                          {_, HB} -> HB
                      end,
    FrameMax = case ClientFrameMax of
                   undefined -> 0;
                   {_, FM} -> FM
               end,
    ServerFrameMax = server_frame_max(),
    State1 =
        if (FrameMax /= 0) and (FrameMax < ?FRAME_MIN_SIZE) ->
                rabbit_misc:protocol_error(
                  not_allowed, "frame_max=~w < ~w min size",
                  [FrameMax, ?FRAME_MIN_SIZE]);
           %% TODO Python client sets 2^32-1
           %% (ServerFrameMax /= 0) and (FrameMax > ServerFrameMax) ->
           %%      rabbit_misc:protocol_error(
           %%        not_allowed, "frame_max=~w > ~w max size",
           %%        [FrameMax, ServerFrameMax]);
           true ->
            SendFun =
                    fun() ->
                            Frame =
                                rabbit_amqp1_0_binary_generator:build_heartbeat_frame(),
                            catch rabbit_net:send(Sock, Frame)
                    end,

                Parent = self(),
                ReceiveFun =
                    fun() ->
                            Parent ! timeout
                    end,
                Heartbeater = SHF(Sock, ClientHeartbeat, SendFun,
                                  ClientHeartbeat, ReceiveFun),
                State#v1{connection_state = running,
                         connection = Connection#connection{
                                        client_properties = ClientProps,
                                        vhost = <<"/">>, %% FIXME relate to hostname
                                        timeout_sec = ClientHeartbeat,
                                        frame_max = FrameMax},
                         heartbeater = Heartbeater}
        end,
    ok = send_on_channel0(
           Sock,
           #'v1_0.open'{channel_max = {ushort, 0},
                        max_frame_size = {uint, FrameMax},
                        container_id = {utf8, list_to_binary(atom_to_list(node()))}},
           rabbit_amqp1_0_framing),
    State2 = internal_conserve_memory(
               rabbit_alarm:register(self(), {?MODULE, conserve_memory, []}),
               State1),
    rabbit_event:notify(connection_created,
                        infos(?CREATION_EVENT_KEYS, State2)),
    rabbit_event:if_enabled(StatsTimer, fun() -> emit_stats(State2) end),
    State2;

handle_1_0_connection_frame(_Frame, State) ->
    lists:foreach(fun rabbit_channel:shutdown/1, all_channels()),
    maybe_close(State#v1{connection_state = closing}).

handle_1_0_session_frame(Channel, Frame, State) ->
    case get({channel, Channel}) of
        {ch_fr_pid, SessionPid} ->
            ok = rabbit_amqp1_0_session:process_frame(SessionPid, Frame),
            case Frame of
                #'v1_0.end'{} ->
                    erase({channel, Channel}),
                    State;
                #'v1_0.transfer'{} ->
                    case (State#v1.connection_state =:= blocking) of
                        true  -> State#v1{connection_state = blocked};
                        false -> State
                    end;
                _ ->
                    State
            end;
        closing ->
            case Frame of
                #'v1_0.end'{} ->
                    erase({channel, Channel});
                _Else ->
                    ok
            end,
            State;
        undefined ->
            case ?IS_RUNNING(State) of
                true ->
                    ok = send_to_new_1_0_session(Channel, Frame, State),
                    State;
                false ->
                    throw({channel_frame_while_starting,
                           Channel, State#v1.connection_state,
                           Frame})
            end
    end.

handle_1_0_sasl_frame(Frame, State) ->
    io:format("SASL frame ~p~n", [Frame]),
    State.

%% End 1-0

handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32>>, State) ->
    ensure_stats_timer(
      switch_callback(State, {frame_payload, Type, Channel, PayloadSize},
                      PayloadSize + 1));

handle_input({frame_payload, Type, Channel, PayloadSize},
             PayloadAndMarker, State) ->
    case PayloadAndMarker of
        <<Payload:PayloadSize/binary, ?FRAME_END>> ->
            switch_callback(handle_frame(Type, Channel, Payload, State),
                            frame_header, 7);
        _ ->
            throw({bad_payload, Type, Channel, PayloadSize, PayloadAndMarker})
    end;

%% Begin 1-0

handle_input({frame_header_1_0, Mode}, <<Size:32, DOff:8, Type:8, Channel:16>>,
             State) when DOff >= 2 andalso Type == 0 ->
    ?DEBUG("1.0 frame header: doff: ~p size: ~p~n", [DOff, Size]),
    case Size of
        8 -> % length inclusive
            {State, {frame_header_1_0, Mode}, 8}; %% heartbeat
        _ ->
            ensure_stats_timer(
              switch_callback(State, {frame_payload_1_0, Mode, DOff, Channel}, Size - 8))
    end;
handle_input({frame_header_1_0, _Mode}, Malformed, _State) ->
    throw({bad_1_0_header, Malformed});
handle_input({frame_payload_1_0, Mode, DOff, Channel},
            FrameBin, State) ->
    SkipBits = (DOff * 32 - 64), % DOff = 4-byte words, we've read 8 already
    <<Skip:SkipBits, FramePayload/binary>> = FrameBin,
    Skip = Skip, %% hide warning when debug is off
    ?DEBUG("1.0 frame: ~p (skipped ~p)~n", [FramePayload, Skip]),
    handle_1_0_frame(Mode, Channel, FramePayload,
                     switch_callback(State, {frame_header_1_0, Mode}, 8));

%% End 1-0

%% The two rules pertaining to version negotiation:
%%
%% * If the server cannot support the protocol specified in the
%% protocol header, it MUST respond with a valid protocol header and
%% then close the socket connection.
%%
%% * The server MUST provide a protocol version that is lower than or
%% equal to that requested by the client in the protocol header.
handle_input(handshake, <<"AMQP", 0, 0, 9, 1>>, State) ->
    start_connection({0, 9, 1}, rabbit_framing_amqp_0_9_1, State);

%% This is the protocol header for 0-9, which we can safely treat as
%% though it were 0-9-1.
handle_input(handshake, <<"AMQP", 1, 1, 0, 9>>, State) ->
    start_connection({0, 9, 0}, rabbit_framing_amqp_0_9_1, State);

%% This is what most clients send for 0-8.  The 0-8 spec, confusingly,
%% defines the version as 8-0.
handle_input(handshake, <<"AMQP", 1, 1, 8, 0>>, State) ->
    start_connection({8, 0, 0}, rabbit_framing_amqp_0_8, State);

%% The 0-8 spec as on the AMQP web site actually has this as the
%% protocol header; some libraries e.g., py-amqplib, send it when they
%% want 0-8.
handle_input(handshake, <<"AMQP", 1, 1, 9, 1>>, State) ->
    start_connection({8, 0, 0}, rabbit_framing_amqp_0_8, State);

%% Begin 1-0

%% ... and finally, the 1.0 spec is crystal clear!  Note that the
%% Protocol supplied is vestigal; we use it as a marker, but not in
%% general where the 0-x code would use it as a module.
%% FIXME TLS uses a different protocol number, and would go here.
handle_input(handshake, H = <<"AMQP", 0, 1, 0, 0>>, State) ->
    start_1_0_connection(amqp, H, {1, 0, 0}, rabbit_amqp1_0_framing, State);

%% 3 stands for "SASL"
handle_input(handshake, H = <<"AMQP", 3, 1, 0, 0>>, State) ->
    io:format("SASL handshake~n"),
    start_1_0_connection(sasl, H, {1, 0, 0}, rabbit_amqp1_0_framing, State);

%% End 1-0

handle_input(handshake, <<"AMQP", A, B, C, D>>, #v1{sock = Sock}) ->
    refuse_connection(Sock, {bad_version, A, B, C, D});

handle_input(handshake, Other, #v1{sock = Sock}) ->
    refuse_connection(Sock, {bad_header, Other});

handle_input(Callback, Data, _State) ->
    throw({bad_input, Callback, Data}).

%% Offer a protocol version to the client.  Connection.start only
%% includes a major and minor version number, Luckily 0-9 and 0-9-1
%% are similar enough that clients will be happy with either.
start_connection({ProtocolMajor, ProtocolMinor, _ProtocolRevision},
                 Protocol,
                 State = #v1{sock = Sock, connection = Connection}) ->
    Start = #'connection.start'{
      version_major = ProtocolMajor,
      version_minor = ProtocolMinor,
      server_properties = server_properties(Protocol),
      mechanisms = auth_mechanisms_binary(Sock),
      locales = <<"en_US">> },
    ok = send_on_channel0(Sock, Start, Protocol),
    switch_callback(State#v1{connection = Connection#connection{
                                            timeout_sec = ?NORMAL_TIMEOUT,
                                            protocol = Protocol},
                             connection_state = starting},
                    frame_header, 7).

%% Begin 1-0

start_1_0_connection(Mode,
                     AMQPABCD,
                     {1, 0, 0},
                     Protocol,
                     State = #v1{sock = Sock, connection = Connection}) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, AMQPABCD) end),
    switch_callback(State#v1{connection = Connection#connection{
                                            timeout_sec = ?NORMAL_TIMEOUT,
                                            protocol = Protocol},
                             connection_state = starting},
                    {frame_header_1_0, Mode}, 8).

%% End 1-0

%% TODO this is a 1-0 issue: if someone tries to connect with e.g. 0-10, what
%% should we tell them we support? The spec says the highest version we have
%% - which is 1-0. Is that TRTTD?
refuse_connection(Sock, Exception) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, <<"AMQP",0,0,9,1>>) end),
    throw(Exception).

ensure_stats_timer(State = #v1{stats_timer = StatsTimer,
                               connection_state = running}) ->
    State#v1{stats_timer = rabbit_event:ensure_stats_timer(
                             StatsTimer, self(), emit_stats)};
ensure_stats_timer(State) ->
    State.

%%--------------------------------------------------------------------------

handle_method0(MethodName, FieldsBin,
               State = #v1{connection = #connection{protocol = Protocol}}) ->
    HandleException =
        fun(R) ->
                case ?IS_RUNNING(State) of
                    true  -> send_exception(State, 0, R);
                    %% We don't trust the client at this point - force
                    %% them to wait for a bit so they can't DOS us with
                    %% repeated failed logins etc.
                    false -> timer:sleep(?SILENT_CLOSE_DELAY * 1000),
                             throw({channel0_error, State#v1.connection_state, R})
                end
        end,
    try
        handle_method0(Protocol:decode_method_fields(MethodName, FieldsBin),
                       State)
    catch exit:#amqp_error{method = none} = Reason ->
            HandleException(Reason#amqp_error{method = MethodName});
          Type:Reason ->
            HandleException({Type, Reason, MethodName, erlang:get_stacktrace()})
    end.

handle_method0(#'connection.start_ok'{mechanism = Mechanism,
                                      response = Response,
                                      client_properties = ClientProperties},
               State0 = #v1{connection_state = starting,
                            connection       = Connection,
                            sock             = Sock}) ->
    AuthMechanism = auth_mechanism_to_module(Mechanism, Sock),
    Capabilities =
        case rabbit_misc:table_lookup(ClientProperties, <<"capabilities">>) of
            {table, Capabilities1} -> Capabilities1;
            _                      -> []
        end,
    State = State0#v1{auth_mechanism   = AuthMechanism,
                      auth_state       = AuthMechanism:init(Sock),
                      connection_state = securing,
                      connection       =
                          Connection#connection{
                            client_properties = ClientProperties,
                            capabilities      = Capabilities}},
    auth_phase(Response, State);

handle_method0(#'connection.secure_ok'{response = Response},
               State = #v1{connection_state = securing}) ->
    auth_phase(Response, State);

handle_method0(#'connection.tune_ok'{frame_max = FrameMax,
                                     heartbeat = ClientHeartbeat},
               State = #v1{connection_state = tuning,
                           connection = Connection,
                           sock = Sock,
                           start_heartbeat_fun = SHF}) ->
    ServerFrameMax = server_frame_max(),
    if FrameMax /= 0 andalso FrameMax < ?FRAME_MIN_SIZE ->
            rabbit_misc:protocol_error(
              not_allowed, "frame_max=~w < ~w min size",
              [FrameMax, ?FRAME_MIN_SIZE]);
       ServerFrameMax /= 0 andalso FrameMax > ServerFrameMax ->
            rabbit_misc:protocol_error(
              not_allowed, "frame_max=~w > ~w max size",
              [FrameMax, ServerFrameMax]);
       true ->
            Frame = rabbit_binary_generator:build_heartbeat_frame(),
            SendFun = fun() -> catch rabbit_net:send(Sock, Frame) end,
            Parent = self(),
            ReceiveFun = fun() -> Parent ! timeout end,
            Heartbeater = SHF(Sock, ClientHeartbeat, SendFun,
                              ClientHeartbeat, ReceiveFun),
            State#v1{connection_state = opening,
                     connection = Connection#connection{
                                    timeout_sec = ClientHeartbeat,
                                    frame_max = FrameMax},
                     heartbeater = Heartbeater}
    end;

handle_method0(#'connection.open'{virtual_host = VHostPath},
               State = #v1{connection_state = opening,
                           connection = Connection = #connection{
                                          user = User,
                                          protocol = Protocol},
                           sock = Sock,
                           stats_timer = StatsTimer}) ->
    ok = rabbit_access_control:check_vhost_access(User, VHostPath),
    NewConnection = Connection#connection{vhost = VHostPath},
    ok = send_on_channel0(Sock, #'connection.open_ok'{}, Protocol),
    State1 = internal_conserve_memory(
               rabbit_alarm:register(self(), {?MODULE, conserve_memory, []}),
               State#v1{connection_state = running,
                        connection = NewConnection}),
    rabbit_event:notify(connection_created,
                        [{type, network} |
                         infos(?CREATION_EVENT_KEYS, State1)]),
    rabbit_event:if_enabled(StatsTimer,
                            fun() -> emit_stats(State1) end),
    State1;
handle_method0(#'connection.close'{}, State) when ?IS_RUNNING(State) ->
    lists:foreach(fun rabbit_channel:shutdown/1, all_channels()),
    maybe_close(State#v1{connection_state = closing});
handle_method0(#'connection.close'{},
               State = #v1{connection_state = CS,
                           connection = #connection{protocol = Protocol},
                           sock = Sock})
  when CS =:= closing; CS =:= closed ->
    %% We're already closed or closing, so we don't need to cleanup
    %% anything.
    ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
    State;
handle_method0(#'connection.close_ok'{},
               State = #v1{connection_state = closed}) ->
    self() ! terminate_connection,
    State;
handle_method0(_Method, State = #v1{connection_state = CS})
  when CS =:= closing; CS =:= closed ->
    State;
handle_method0(_Method, #v1{connection_state = S}) ->
    rabbit_misc:protocol_error(
      channel_error, "unexpected method in connection state ~w", [S]).

%% Compute frame_max for this instance. Could simply use 0, but breaks
%% QPid Java client.
server_frame_max() ->
    {ok, FrameMax} = application:get_env(rabbit, frame_max),
    FrameMax.

%% Begin 1-0
send_on_channel0(Sock, Method, Protocol) ->
    ok = rabbit_amqp1_0_writer:internal_send_command(Sock, 0, Method, Protocol).
%% End 1-0

auth_mechanism_to_module(TypeBin, Sock) ->
    case rabbit_registry:binary_to_type(TypeBin) of
        {error, not_found} ->
            rabbit_misc:protocol_error(
              command_invalid, "unknown authentication mechanism '~s'",
              [TypeBin]);
        T ->
            case {lists:member(T, auth_mechanisms(Sock)),
                  rabbit_registry:lookup_module(auth_mechanism, T)} of
                {true, {ok, Module}} ->
                    Module;
                _ ->
                    rabbit_misc:protocol_error(
                      command_invalid,
                      "invalid authentication mechanism '~s'", [T])
            end
    end.

auth_mechanisms(Sock) ->
    {ok, Configured} = application:get_env(auth_mechanisms),
    [Name || {Name, Module} <- rabbit_registry:lookup_all(auth_mechanism),
             Module:should_offer(Sock), lists:member(Name, Configured)].

auth_mechanisms_binary(Sock) ->
    list_to_binary(
      string:join([atom_to_list(A) || A <- auth_mechanisms(Sock)], " ")).

auth_phase(Response,
           State = #v1{auth_mechanism = AuthMechanism,
                       auth_state = AuthState,
                       connection = Connection =
                           #connection{protocol = Protocol},
                       sock = Sock}) ->
    case AuthMechanism:handle_response(Response, AuthState) of
        {refused, Msg, Args} ->
            rabbit_misc:protocol_error(
              access_refused, "~s login refused: ~s",
              [proplists:get_value(name, AuthMechanism:description()),
               io_lib:format(Msg, Args)]);
        {protocol_error, Msg, Args} ->
            rabbit_misc:protocol_error(syntax_error, Msg, Args);
        {challenge, Challenge, AuthState1} ->
            Secure = #'connection.secure'{challenge = Challenge},
            ok = send_on_channel0(Sock, Secure, Protocol),
            State#v1{auth_state = AuthState1};
        {ok, User} ->
            Tune = #'connection.tune'{channel_max = 0,
                                      frame_max = server_frame_max(),
                                      heartbeat = 0},
            ok = send_on_channel0(Sock, Tune, Protocol),
            State#v1{connection_state = tuning,
                     connection = Connection#connection{user = User}}
    end.

%%--------------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(pid, #v1{}) ->
    self();
i(address, #v1{sock = Sock}) ->
    socket_info(fun rabbit_net:sockname/1, fun ({A, _}) -> A end, Sock);
i(port, #v1{sock = Sock}) ->
    socket_info(fun rabbit_net:sockname/1, fun ({_, P}) -> P end, Sock);
i(peer_address, #v1{sock = Sock}) ->
    socket_info(fun rabbit_net:peername/1, fun ({A, _}) -> A end, Sock);
i(peer_port, #v1{sock = Sock}) ->
    socket_info(fun rabbit_net:peername/1, fun ({_, P}) -> P end, Sock);
i(ssl, #v1{sock = Sock}) ->
    rabbit_net:is_ssl(Sock);
i(ssl_protocol, #v1{sock = Sock}) ->
    ssl_info(fun ({P, _}) -> P end, Sock);
i(ssl_key_exchange, #v1{sock = Sock}) ->
    ssl_info(fun ({_, {K, _, _}}) -> K end, Sock);
i(ssl_cipher, #v1{sock = Sock}) ->
    ssl_info(fun ({_, {_, C, _}}) -> C end, Sock);
i(ssl_hash, #v1{sock = Sock}) ->
    ssl_info(fun ({_, {_, _, H}}) -> H end, Sock);
i(peer_cert_issuer, #v1{sock = Sock}) ->
    cert_info(fun rabbit_ssl:peer_cert_issuer/1, Sock);
i(peer_cert_subject, #v1{sock = Sock}) ->
    cert_info(fun rabbit_ssl:peer_cert_subject/1, Sock);
i(peer_cert_validity, #v1{sock = Sock}) ->
    cert_info(fun rabbit_ssl:peer_cert_validity/1, Sock);
i(SockStat, #v1{sock = Sock}) when SockStat =:= recv_oct;
                                   SockStat =:= recv_cnt;
                                   SockStat =:= send_oct;
                                   SockStat =:= send_cnt;
                                   SockStat =:= send_pend ->
    socket_info(fun () -> rabbit_net:getstat(Sock, [SockStat]) end,
                fun ([{_, I}]) -> I end);
i(state, #v1{connection_state = S}) ->
    S;
i(channels, #v1{}) ->
    length(all_channels());
i(protocol, #v1{connection = #connection{protocol = none}}) ->
    none;
i(protocol, #v1{connection = #connection{protocol = Protocol}}) ->
    Protocol:version();
i(auth_mechanism, #v1{auth_mechanism = none}) ->
    none;
i(auth_mechanism, #v1{auth_mechanism = Mechanism}) ->
    proplists:get_value(name, Mechanism:description());
i(user, #v1{connection = #connection{user = #user{username = Username}}}) ->
    Username;
i(user, #v1{connection = #connection{user = none}}) ->
    '';
i(vhost, #v1{connection = #connection{vhost = VHost}}) ->
    VHost;
i(timeout, #v1{connection = #connection{timeout_sec = Timeout}}) ->
    Timeout;
i(frame_max, #v1{connection = #connection{frame_max = FrameMax}}) ->
    FrameMax;
i(client_properties, #v1{connection = #connection{
                           client_properties = ClientProperties}}) ->
    ClientProperties;
i(Item, #v1{}) ->
    throw({bad_argument, Item}).

socket_info(Get, Select, Sock) ->
    socket_info(fun() -> Get(Sock) end, Select).

socket_info(Get, Select) ->
    case Get() of
        {ok,    T} -> Select(T);
        {error, _} -> ''
    end.

ssl_info(F, Sock) ->
    %% The first ok form is R14
    %% The second is R13 - the extra term is exportability (by inspection,
    %% the docs are wrong)
    case rabbit_net:ssl_info(Sock) of
        nossl                   -> '';
        {error, _}              -> '';
        {ok, {P, {K, C, H}}}    -> F({P, {K, C, H}});
        {ok, {P, {K, C, H, _}}} -> F({P, {K, C, H}})
    end.

cert_info(F, Sock) ->
    case rabbit_net:peercert(Sock) of
        nossl                -> '';
        {error, no_peercert} -> '';
        {ok, Cert}           -> list_to_binary(F(Cert))
    end.

%%--------------------------------------------------------------------------

send_to_new_channel(Channel, AnalyzedFrame, State) ->
    #v1{sock = Sock, queue_collector = Collector,
        channel_sup_sup_pid = ChanSupSup,
        connection = #connection{protocol     = Protocol,
                                 frame_max    = FrameMax,
                                 user         = User,
                                 vhost        = VHost,
                                 capabilities = Capabilities}} = State,
    {ok, _ChSupPid, {ChPid, AState}} =
        rabbit_channel_sup_sup:start_channel(
          ChanSupSup, {tcp, Sock, Channel, FrameMax, self(), Protocol, User,
                       VHost, Capabilities, Collector}),
    MRef = erlang:monitor(process, ChPid),
    NewAState = process_channel_frame(AnalyzedFrame, self(),
                                      Channel, ChPid, AState),
    put({channel, Channel}, {ChPid, NewAState}),
    put({ch_pid, ChPid}, {Channel, MRef}),
    State.

%% Begin 1-0

send_to_new_1_0_session(Channel, Frame, State) ->
    #v1{sock = Sock, queue_collector = Collector,
        channel_sup_sup_pid = ChanSupSup,
        connection = #connection{protocol  = Protocol,
                                 frame_max = FrameMax,
                                 %% FIXME SASL, TLS, etc.
                                 user      = User,
                                 vhost     = VHost}} = State,
    {ok, ChSupPid, ChFrPid} =
        %% Note: the equivalent, start_channel is in channel_sup_sup
        rabbit_amqp1_0_session_sup_sup:start_session(
          ChanSupSup, {Protocol, Sock, Channel, FrameMax,
                       self(), User, VHost, Collector}),
    erlang:monitor(process, ChFrPid),
    put({channel, Channel}, {ch_fr_pid, ChFrPid}),
    put({ch_sup_pid, ChSupPid}, {{channel, Channel}, {ch_fr_pid, ChFrPid}}),
    put({ch_fr_pid, ChFrPid}, {channel, Channel}),
    ok = rabbit_amqp1_0_session:process_frame(ChFrPid, Frame).

%% End 1-0

process_channel_frame(Frame, ErrPid, Channel, ChPid, AState) ->
    case rabbit_command_assembler:process(Frame, AState) of
        {ok, NewAState}                  -> NewAState;
        {ok, Method, NewAState}          -> rabbit_channel:do(ChPid, Method),
                                            NewAState;
        {ok, Method, Content, NewAState} -> rabbit_channel:do(ChPid,
                                                              Method, Content),
                                            NewAState;
        {error, Reason}                  -> ErrPid ! {channel_exit, Channel,
                                                      Reason},
                                            AState
    end.

handle_exception(State = #v1{connection_state = closed}, _Channel, _Reason) ->
    State;
handle_exception(State, Channel, Reason) ->
    send_exception(State, Channel, Reason).

send_exception(State = #v1{connection = #connection{protocol = Protocol}},
               Channel, Reason) ->
    {0, CloseMethod} =
        rabbit_binary_generator:map_exception(Channel, Reason, Protocol),
    terminate_channels(),
    State1 = close_connection(State),
    ok = rabbit_writer:internal_send_command(
           State1#v1.sock, 0, CloseMethod, Protocol),
    State1.

emit_stats(State = #v1{stats_timer = StatsTimer}) ->
    rabbit_event:notify(connection_stats, infos(?STATISTICS_KEYS, State)),
    State#v1{stats_timer = rabbit_event:reset_stats_timer(StatsTimer)}.
