%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_reader).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("kernel/include/inet.hrl").
-include("rabbit_amqp1_0.hrl").

-export([init/2, mainloop/2]).
-export([info/2]).

%% TODO which of these are needed?
-export([shutdown/2]).
-export([system_continue/3, system_terminate/4, system_code_change/4]).
-export([conserve_resources/3]).

-import(rabbit_amqp1_0_util, [protocol_error/3]).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 30).
-define(SILENT_CLOSE_DELAY, 3).

%%--------------------------------------------------------------------------

-record(v1, {parent, sock, connection, callback, recv_len, pending_recv,
             connection_state, queue_collector, heartbeater, helper_sup,
             channel_sup_sup_pid, buf, buf_len, throttle, proxy_socket}).

-record(v1_connection, {user, timeout_sec, frame_max, auth_mechanism, auth_state,
                        hostname}).

-record(throttle, {alarmed_by, last_blocked_by, last_blocked_at}).

-define(IS_RUNNING(State),
        (State#v1.connection_state =:= running orelse
         State#v1.connection_state =:= blocking orelse
         State#v1.connection_state =:= blocked)).

%%--------------------------------------------------------------------------

unpack_from_0_9_1({Parent, Sock,RecvLen, PendingRecv,
                   HelperSupPid, Buf, BufLen, ProxySocket}) ->
    #v1{parent              = Parent,
        sock                = Sock,
        callback            = handshake,
        recv_len            = RecvLen,
        pending_recv        = PendingRecv,
        connection_state    = pre_init,
        queue_collector     = undefined,
        heartbeater         = none,
        helper_sup          = HelperSupPid,
        buf                 = Buf,
        buf_len             = BufLen,
        throttle = #throttle{alarmed_by      = [],
                             last_blocked_by = none,
                             last_blocked_at = never},
        connection = #v1_connection{user           = none,
                                    timeout_sec    = ?HANDSHAKE_TIMEOUT,
                                    frame_max      = ?FRAME_MIN_SIZE,
                                    auth_mechanism = none,
                                    auth_state     = none},
        proxy_socket = ProxySocket}.

shutdown(Pid, Explanation) ->
    gen_server:call(Pid, {shutdown, Explanation}, infinity).

system_continue(Parent, Deb, State) ->
    ?MODULE:mainloop(Deb, State#v1{parent = Parent}).

system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

conserve_resources(Pid, Source, {_, Conserve, _}) ->
    Pid ! {conserve_resources, Source, Conserve},
    ok.

server_properties() ->
    %% The atom doesn't match anything, it's just "not 0-9-1".
    Raw = lists:keydelete(
          <<"capabilities">>, 1, rabbit_reader:server_properties(amqp_1_0)),
    {map, [{{symbol, K}, {utf8, V}} || {K, longstr, V}  <- Raw]}.

%%--------------------------------------------------------------------------

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

recvloop(Deb, State = #v1{pending_recv = true}) ->
    mainloop(Deb, State);
recvloop(Deb, State = #v1{connection_state = blocked}) ->
    mainloop(Deb, State);
recvloop(Deb, State = #v1{sock = Sock, recv_len = RecvLen, buf_len = BufLen})
  when BufLen < RecvLen ->
    case rabbit_net:setopts(Sock, [{active, once}]) of
        ok ->
            mainloop(Deb, State#v1{pending_recv = true});
        {error, Reason} ->
            throw({inet_error, Reason})
    end;
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
        {data, Data} ->
            recvloop(Deb, State#v1{buf = [Data | Buf],
                                   buf_len = BufLen + size(Data),
                                   pending_recv = false});
        closed when State#v1.connection_state =:= closed ->
            ok;
        closed ->
            throw(connection_closed_abruptly);
        {error, Reason} ->
            throw({inet_error, Reason});
        {other, {system, From, Request}} ->
            sys:handle_system_msg(Request, From, State#v1.parent,
                                  ?MODULE, Deb, State);
        {other, Other} ->
            case handle_other(Other, State) of
                stop     -> ok;
                NewState -> recvloop(Deb, NewState)
            end
    end.

handle_other({conserve_resources, Source, Conserve},
             State = #v1{throttle = Throttle =
                             #throttle{alarmed_by = CR}}) ->
    CR1 = case Conserve of
              true  -> lists:usort([Source | CR]);
              false -> CR -- [Source]
          end,
    Throttle1 = Throttle#throttle{alarmed_by = CR1},
    control_throttle(State#v1{throttle = Throttle1});
handle_other({'EXIT', Parent, Reason}, State = #v1{parent = Parent}) ->
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
handle_other({'DOWN', _MRef, process, ChPid, Reason}, State) ->
    handle_dependent_exit(ChPid, Reason, State);
handle_other(handshake_timeout, State)
  when ?IS_RUNNING(State) orelse
       State#v1.connection_state =:= closing orelse
       State#v1.connection_state =:= closed ->
    State;
handle_other(handshake_timeout, State) ->
    throw({handshake_timeout, State#v1.callback});
handle_other(heartbeat_timeout, State = #v1{connection_state = closed}) ->
    State;
handle_other(heartbeat_timeout, #v1{connection_state = S}) ->
    throw({heartbeat_timeout, S});
handle_other({'$gen_call', From, {shutdown, Explanation}}, State) ->
    {ForceTermination, NewState} = terminate(Explanation, State),
    gen_server:reply(From, ok),
    case ForceTermination of
        force  -> stop;
        normal -> NewState
    end;
handle_other({'$gen_cast', force_event_refresh}, State) ->
    %% Ignore, the broker sent us this as it thinks we are a 0-9-1 connection
    State;
handle_other({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    control_throttle(State);
handle_other(terminate_connection, State) ->
    State;
handle_other({info, InfoItems, Pid}, State) ->
    Infos = lists:map(
              fun(InfoItem) ->
                      {InfoItem, info_internal(InfoItem, State)}
              end,
              InfoItems),
    Pid ! {info_reply, Infos},
    State;
handle_other(Other, _State) ->
    %% internal error -> something worth dying for
    exit({unexpected_message, Other}).

switch_callback(State, Callback, Length) ->
    State#v1{callback = Callback, recv_len = Length}.

terminate(Reason, State) when ?IS_RUNNING(State) ->
    {normal, handle_exception(State, 0,
                              {?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                               "Connection forced: ~p", [Reason]})};
terminate(_Reason, State) ->
    {force, State}.

control_throttle(State = #v1{connection_state = CS, throttle = Throttle}) ->
    IsThrottled = ((Throttle#throttle.alarmed_by =/= []) orelse
               credit_flow:blocked()),
    case {CS, IsThrottled} of
        {running,   true} -> State#v1{connection_state = blocking};
        {blocking, false} -> State#v1{connection_state = running};
        {blocked,  false} -> ok = rabbit_heartbeat:resume_monitor(
                                    State#v1.heartbeater),
                             State#v1{connection_state = running};
        {blocked,   true} -> State#v1{throttle = update_last_blocked_by(
                                                   Throttle)};
        {_,            _} -> State
    end.

update_last_blocked_by(Throttle = #throttle{alarmed_by = []}) ->
    Throttle#throttle{last_blocked_by = flow};
update_last_blocked_by(Throttle) ->
    Throttle#throttle{last_blocked_by = resource}.

%%--------------------------------------------------------------------------
%% error handling / termination

close_connection(State = #v1{connection = #v1_connection{
                                             timeout_sec = TimeoutSec}}) ->
    erlang:send_after((if TimeoutSec > 0 andalso
                          TimeoutSec < ?CLOSING_TIMEOUT -> TimeoutSec;
                          true                          -> ?CLOSING_TIMEOUT
                       end) * 1000, self(), terminate_connection),
    State#v1{connection_state = closed}.

handle_dependent_exit(ChPid, Reason, State) ->
    case {ChPid, termination_kind(Reason)} of
        {undefined, uncontrolled} ->
            exit({abnormal_dependent_exit, ChPid, Reason});
        {_Channel, controlled} ->
            maybe_close(control_throttle(State));
        {Channel, uncontrolled} ->
            {RealReason, Trace} = Reason,
            R = {?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                 "Session error: ~p~n~p", [RealReason, Trace]},
            maybe_close(handle_exception(control_throttle(State), Channel, R))
    end.

termination_kind(normal) -> controlled;
termination_kind(_)      -> uncontrolled.

maybe_close(State = #v1{connection_state = closing,
                        sock = Sock}) ->
    NewState = close_connection(State),
    ok = send_on_channel0(Sock, #'v1_0.close'{}),
    % Perform an rpc call to each session process to allow it time to
    % process it's internal message buffer before the supervision tree
    % shuts everything down and in flight messages such as dispositions
    % could be lost
    [ _ = rabbit_amqp1_0_session:get_info(SessionPid)
      || {{channel, _}, {ch_fr_pid, SessionPid}} <- get()],
    NewState;
maybe_close(State) ->
    State.

error_frame(Condition, Fmt, Args) ->
    #'v1_0.error'{condition = Condition,
                  description = {utf8, list_to_binary(
                                         rabbit_misc:format(Fmt, Args))}}.

handle_exception(State = #v1{connection_state = closed}, Channel,
                 #'v1_0.error'{description = {utf8, Desc}}) ->
    rabbit_log_connection:error("Error on AMQP 1.0 connection ~p (~p), channel ~p:~n~p",
        [self(), closed, Channel, Desc]),
    State;
handle_exception(State = #v1{connection_state = CS}, Channel,
                 ErrorFrame = #'v1_0.error'{description = {utf8, Desc}})
  when ?IS_RUNNING(State) orelse CS =:= closing ->
    rabbit_log_connection:error("Error on AMQP 1.0 connection ~p (~p), channel ~p:~n~p",
        [self(), CS, Channel, Desc]),
    %% TODO: session errors shouldn't force the connection to close
    State1 = close_connection(State),
    ok = send_on_channel0(State#v1.sock, #'v1_0.close'{error = ErrorFrame}),
    State1;
handle_exception(State, Channel, Error) ->
    %% We don't trust the client at this point - force them to wait
    %% for a bit so they can't DOS us with repeated failed logins etc.
    timer:sleep(?SILENT_CLOSE_DELAY * 1000),
    throw({handshake_error, State#v1.connection_state, Channel, Error}).

%%--------------------------------------------------------------------------

%% Begin 1-0

%% ----------------------------------------
%% AMQP 1.0 frame handlers

is_connection_frame(#'v1_0.open'{})  -> true;
is_connection_frame(#'v1_0.close'{}) -> true;
is_connection_frame(_)               -> false.

%% TODO Handle depending on connection state
%% TODO It'd be nice to only decode up to the descriptor

handle_1_0_frame(Mode, Channel, Payload, State) ->
    try
        handle_1_0_frame0(Mode, Channel, Payload, State)
    catch
        _:#'v1_0.error'{} = Reason ->
            handle_exception(State, 0, Reason);
        _:{error, {not_allowed, Username}} ->
            %% section 2.8.15 in http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-complete-v1.0-os.pdf
            handle_exception(State, 0, error_frame(
                                         ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                                         "Access for user '~s' was refused: insufficient permissions", [Username]));
        _:Reason:Trace ->
            handle_exception(State, 0, error_frame(
                                         ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                         "Reader error: ~p~n~p",
                                         [Reason, Trace]))
    end.

%% Nothing specifies that connection methods have to be on a
%% particular channel.
handle_1_0_frame0(_Mode, Channel, Payload,
                 State = #v1{ connection_state = CS}) when
      CS =:= closing; CS =:= closed ->
    Sections = parse_1_0_frame(Payload, Channel),
    case is_connection_frame(Sections) of
        true  -> handle_1_0_connection_frame(Sections, State);
        false -> State
    end;
handle_1_0_frame0(Mode, Channel, Payload, State) ->
    Sections = parse_1_0_frame(Payload, Channel),
    case {Mode, is_connection_frame(Sections)} of
        {amqp, true}  -> handle_1_0_connection_frame(Sections, State);
        {amqp, false} -> handle_1_0_session_frame(Channel, Sections, State);
        {sasl, false} -> handle_1_0_sasl_frame(Sections, State)
    end.

parse_1_0_frame(Payload, _Channel) ->
    {PerfDesc, Rest} = amqp10_binary_parser:parse(Payload),
    Perf = amqp10_framing:decode(PerfDesc),
    ?DEBUG("Channel ~p ->~n~p~n~s",
           [_Channel, amqp10_framing:pprint(Perf),
            case Rest of
                <<>> -> <<>>;
                _    -> rabbit_misc:format(
                          "  followed by ~p bytes of content", [size(Rest)])
            end]),
    case Rest of
        <<>> -> Perf;
        _    -> {Perf, Rest}
    end.

handle_1_0_connection_frame(#'v1_0.open'{ max_frame_size = ClientFrameMax,
                                          channel_max = ClientChannelMax,
                                          idle_time_out = IdleTimeout,
                                          hostname = Hostname },
                            State = #v1{
                              connection_state = starting,
                              connection = Connection,
                              throttle   = Throttle,
                              helper_sup = HelperSupPid,
                              sock = Sock}) ->
    ClientHeartbeatSec = case IdleTimeout of
                             undefined        -> 0;
                             {uint, Interval} -> Interval div 1000
                         end,
    FrameMax           = case ClientFrameMax of
                             undefined -> unlimited;
                             {_, FM}   -> FM
                         end,
    {ok, HeartbeatSec} = application:get_env(rabbit, heartbeat),
    State1 =
        if (FrameMax =/= unlimited) and (FrameMax < ?FRAME_1_0_MIN_SIZE) ->
                protocol_error(?V_1_0_AMQP_ERROR_FRAME_SIZE_TOO_SMALL,
                               "frame_max=~w < ~w min size",
                               [FrameMax, ?FRAME_1_0_MIN_SIZE]);
           true ->
                {ok, Collector} =
                    rabbit_connection_helper_sup:start_queue_collector(
                      HelperSupPid, <<"AMQP 1.0">>), %% TODO describe the connection
                SendFun =
                    fun() ->
                            Frame =
                                amqp10_binary_generator:build_heartbeat_frame(),
                            catch rabbit_net:send(Sock, Frame)
                    end,

                Parent = self(),
                ReceiveFun =
                    fun() ->
                            Parent ! heartbeat_timeout
                    end,
                %% [2.4.5] the value in idle-time-out SHOULD be half the peer's
                %%         actual timeout threshold
                ReceiverHeartbeatSec = lists:min([HeartbeatSec * 2, 4294967]),
                %% TODO: only start heartbeat receive timer at next next frame
                Heartbeater =
                    rabbit_heartbeat:start(HelperSupPid, Sock,
                                           ClientHeartbeatSec, SendFun,
                                           ReceiverHeartbeatSec, ReceiveFun),
                State#v1{connection_state = running,
                         connection = Connection#v1_connection{
                                                   frame_max = FrameMax,
                                                   hostname  = Hostname},
                         heartbeater = Heartbeater,
                         queue_collector = Collector}
        end,
    HostnameVal = case Hostname of
                    undefined -> undefined;
                    {utf8, Val} -> Val
                  end,
    rabbit_log:debug("AMQP 1.0 connection.open frame: hostname = ~s, extracted vhost = ~s, idle_timeout = ~p" ,
                    [HostnameVal, vhost(Hostname), HeartbeatSec * 1000]),
    %% TODO enforce channel_max
    ok = send_on_channel0(
           Sock,
           #'v1_0.open'{channel_max    = ClientChannelMax,
                        max_frame_size = ClientFrameMax,
                        idle_time_out  = {uint, HeartbeatSec * 1000},
                        container_id   = {utf8, rabbit_nodes:cluster_name()},
                        properties     = server_properties()}),
    Conserve = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    control_throttle(
      State1#v1{throttle = Throttle#throttle{alarmed_by = Conserve}});

handle_1_0_connection_frame(_Frame, State) ->
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
                        true ->
                            ok = rabbit_heartbeat:pause_monitor(
                                   State#v1.heartbeater),
                            State#v1{connection_state = blocked};
                        false ->
                            State
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

%% TODO: write a proper ANONYMOUS plugin and unify with STOMP
handle_1_0_sasl_frame(#'v1_0.sasl_init'{mechanism = {symbol, <<"ANONYMOUS">>},
                                        hostname = _Hostname},
                      State = #v1{connection_state = starting,
                                  sock             = Sock}) ->
    case application:get_env(rabbitmq_amqp1_0, default_user) of
        {ok, none} ->
            %% No need to do anything, we will blow up in start_connection
            ok;
        {ok, _} ->
            %% We only need to send the frame, again start_connection
            %% will set up the default user.
            Outcome = #'v1_0.sasl_outcome'{code = {ubyte, 0}},
            ok = send_on_channel0(Sock, Outcome, rabbit_amqp1_0_sasl),
            switch_callback(State#v1{connection_state = waiting_amqp0100},
                            handshake, 8)
    end;
handle_1_0_sasl_frame(#'v1_0.sasl_init'{mechanism        = {symbol, Mechanism},
                                        initial_response = {binary, Response},
                                        hostname         = _Hostname},
                      State0 = #v1{connection_state = starting,
                                   connection       = Connection,
                                   sock             = Sock}) ->
    AuthMechanism = auth_mechanism_to_module(Mechanism, Sock),
    State = State0#v1{connection       =
                          Connection#v1_connection{
                            auth_mechanism    = {Mechanism, AuthMechanism},
                            auth_state        = AuthMechanism:init(Sock)},
                      connection_state = securing},
    auth_phase_1_0(Response, State);
handle_1_0_sasl_frame(#'v1_0.sasl_response'{response = {binary, Response}},
                      State = #v1{connection_state = securing}) ->
    auth_phase_1_0(Response, State);
handle_1_0_sasl_frame(Frame, State) ->
    throw({unexpected_1_0_sasl_frame, Frame, State}).

%% We need to handle restarts...
handle_input(handshake, <<"AMQP", 0, 1, 0, 0>>, State) ->
    start_1_0_connection(amqp, State);

%% 3 stands for "SASL" (keeping this here for when we do TLS)
handle_input(handshake, <<"AMQP", 3, 1, 0, 0>>, State) ->
    start_1_0_connection(sasl, State);

handle_input({frame_header_1_0, Mode},
             Header = <<Size:32, DOff:8, Type:8, Channel:16>>,
             State) when DOff >= 2 ->
    case {Mode, Type} of
        {amqp, 0} -> ok;
        {sasl, 1} -> ok;
        _         -> throw({bad_1_0_header_type, Header, Mode})
    end,
    case Size of
        8 -> % length inclusive
            State; %% heartbeat
        _ ->
            switch_callback(State, {frame_payload_1_0, Mode, DOff, Channel}, Size - 8)
    end;
handle_input({frame_header_1_0, _Mode}, Malformed, _State) ->
    throw({bad_1_0_header, Malformed});
handle_input({frame_payload_1_0, Mode, DOff, Channel},
            FrameBin, State) ->
    SkipBits = (DOff * 32 - 64), % DOff = 4-byte words, we've read 8 already
    <<Skip:SkipBits, FramePayload/binary>> = FrameBin,
    Skip = Skip, %% hide warning when debug is off
    handle_1_0_frame(Mode, Channel, FramePayload,
                     switch_callback(State, {frame_header_1_0, Mode}, 8));

handle_input(Callback, Data, _State) ->
    throw({bad_input, Callback, Data}).

init(Mode, PackedState) ->
    %% By invoking recvloop here we become 1.0.
    recvloop(sys:debug_options([]),
             start_1_0_connection(Mode, unpack_from_0_9_1(PackedState))).

start_1_0_connection(sasl, State = #v1{sock = Sock}) ->
    send_1_0_handshake(Sock, <<"AMQP",3,1,0,0>>),
    Ms = {array, symbol,
          case application:get_env(rabbitmq_amqp1_0, default_user)  of
              {ok, none} -> [];
              {ok, _}    -> [{symbol, <<"ANONYMOUS">>}]
          end ++
              [{symbol, list_to_binary(atom_to_list(M))} || M <- auth_mechanisms(Sock)]},
    Mechanisms = #'v1_0.sasl_mechanisms'{sasl_server_mechanisms = Ms},
    ok = send_on_channel0(Sock, Mechanisms, rabbit_amqp1_0_sasl),
    start_1_0_connection0(sasl, State);

start_1_0_connection(amqp,
                     State = #v1{sock       = Sock,
                                 connection = C = #v1_connection{user = User}}) ->
    {ok, NoAuthUsername} = application:get_env(rabbitmq_amqp1_0, default_user),
    case {User, NoAuthUsername} of
        {none, none} ->
            send_1_0_handshake(Sock, <<"AMQP",3,1,0,0>>),
            throw(banned_unauthenticated_connection);
        {none, Username} ->
            case rabbit_access_control:check_user_login(
                   list_to_binary(Username), []) of
                {ok, NoAuthUser} ->
                    State1 = State#v1{
                               connection = C#v1_connection{user = NoAuthUser}},
                    send_1_0_handshake(Sock, <<"AMQP",0,1,0,0>>),
                    start_1_0_connection0(amqp, State1);
                _ ->
                    send_1_0_handshake(Sock, <<"AMQP",3,1,0,0>>),
                    throw(default_user_missing)
            end;
        _ ->
            send_1_0_handshake(Sock, <<"AMQP",0,1,0,0>>),
            start_1_0_connection0(amqp, State)
    end.

start_1_0_connection0(Mode, State = #v1{connection = Connection,
                                        helper_sup = HelperSup}) ->
    ChannelSupSupPid =
        case Mode of
            sasl -> undefined;
            amqp -> {ok, Pid} =
                        supervisor2:start_child(
                          HelperSup,
                          {channel_sup_sup,
                           {rabbit_amqp1_0_session_sup_sup, start_link, []},
                           intrinsic, infinity, supervisor,
                           [rabbit_amqp1_0_session_sup_sup]}),
                    Pid
        end,
    switch_callback(State#v1{connection = Connection#v1_connection{
                                            timeout_sec = ?NORMAL_TIMEOUT},
                             channel_sup_sup_pid = ChannelSupSupPid,
                             connection_state = starting},
                    {frame_header_1_0, Mode}, 8).

send_1_0_handshake(Sock, Handshake) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, Handshake) end).

send_on_channel0(Sock, Method) ->
    send_on_channel0(Sock, Method, amqp10_framing).

send_on_channel0(Sock, Method, Framing) ->
    ok = rabbit_amqp1_0_writer:internal_send_command(
           Sock, 0, Method, Framing).

%% End 1-0

auth_mechanism_to_module(TypeBin, Sock) ->
    case rabbit_registry:binary_to_type(TypeBin) of
        {error, not_found} ->
            protocol_error(?V_1_0_AMQP_ERROR_NOT_FOUND,
                           "unknown authentication mechanism '~s'", [TypeBin]);
        T ->
            case {lists:member(T, auth_mechanisms(Sock)),
                  rabbit_registry:lookup_module(auth_mechanism, T)} of
                {true, {ok, Module}} ->
                    Module;
                _ ->
                    protocol_error(?V_1_0_AMQP_ERROR_NOT_FOUND,
                                   "invalid authentication mechanism '~s'", [T])
            end
    end.

auth_mechanisms(Sock) ->
    {ok, Configured} = application:get_env(rabbit, auth_mechanisms),
    [Name || {Name, Module} <- rabbit_registry:lookup_all(auth_mechanism),
             Module:should_offer(Sock), lists:member(Name, Configured)].

%% Begin 1-0

auth_phase_1_0(Response,
               State = #v1{connection = Connection =
                               #v1_connection{auth_mechanism = {Name, AuthMechanism},
                                              auth_state     = AuthState},
                       sock = Sock}) ->
    case AuthMechanism:handle_response(Response, AuthState) of
        {refused, User, Msg, Args} ->
            %% We don't trust the client at this point - force them to wait
            %% for a bit before sending the sasl outcome frame
            %% so they can't DOS us with repeated failed logins etc.
            rabbit_core_metrics:auth_attempt_failed(<<>>, User, amqp10),
            timer:sleep(?SILENT_CLOSE_DELAY * 1000),
            Outcome = #'v1_0.sasl_outcome'{code = {ubyte, 1}},
            ok = send_on_channel0(Sock, Outcome, rabbit_amqp1_0_sasl),
            protocol_error(
              ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, "~s login refused: ~s",
              [Name, io_lib:format(Msg, Args)]);
        {protocol_error, Msg, Args} ->
            rabbit_core_metrics:auth_attempt_failed(<<>>, <<>>, amqp10),
            protocol_error(?V_1_0_AMQP_ERROR_DECODE_ERROR, Msg, Args);
        {challenge, Challenge, AuthState1} ->
            rabbit_core_metrics:auth_attempt_succeeded(<<>>, <<>>, amqp10),
            Secure = #'v1_0.sasl_challenge'{challenge = {binary, Challenge}},
            ok = send_on_channel0(Sock, Secure, rabbit_amqp1_0_sasl),
            State#v1{connection = Connection =
                         #v1_connection{auth_state = AuthState1}};
        {ok, User = #user{username = Username}} ->
            case rabbit_access_control:check_user_loopback(Username, Sock) of
                ok ->
                    rabbit_core_metrics:auth_attempt_succeeded(<<>>, Username, amqp10),
                    ok;
                not_allowed ->
                    rabbit_core_metrics:auth_attempt_failed(<<>>, Username, amqp10),
                    protocol_error(
                      ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                      "user '~s' can only connect via localhost",
                      [Username])
            end,
            Outcome = #'v1_0.sasl_outcome'{code = {ubyte, 0}},
            ok = send_on_channel0(Sock, Outcome, rabbit_amqp1_0_sasl),
            switch_callback(
              State#v1{connection_state = waiting_amqp0100,
                       connection = Connection#v1_connection{user = User}},
              handshake, 8)
    end.

send_to_new_1_0_session(Channel, Frame, State) ->
    #v1{sock = Sock, queue_collector = Collector,
        channel_sup_sup_pid = ChanSupSup,
        connection = #v1_connection{frame_max = FrameMax,
                                    hostname  = Hostname,
                                    user      = User},
        proxy_socket = ProxySocket} = State,
    %% Note: the equivalent, start_channel is in channel_sup_sup
    case rabbit_amqp1_0_session_sup_sup:start_session(
           %% NB subtract fixed frame header size
           ChanSupSup, {amqp10_framing, Sock, Channel,
                        case FrameMax of
                            unlimited -> unlimited;
                            _         -> FrameMax - 8
                        end,
                        self(), User, vhost(Hostname), Collector, ProxySocket}) of
        {ok, ChSupPid, ChFrPid} ->
            erlang:monitor(process, ChFrPid),
            put({channel, Channel}, {ch_fr_pid, ChFrPid}),
            put({ch_sup_pid, ChSupPid}, {{channel, Channel}, {ch_fr_pid, ChFrPid}}),
            put({ch_fr_pid, ChFrPid}, {channel, Channel}),
            ok = rabbit_amqp1_0_session:process_frame(ChFrPid, Frame);
        {error, {not_allowed, _}} ->
            rabbit_log:error("AMQP 1.0: user '~s' is not allowed to access virtual host '~s'",
                [User#user.username, vhost(Hostname)]),
            %% Let's skip the supervisor trace, this is an expected error
            throw({error, {not_allowed, User#user.username}});
        {error, _} = E ->
            throw(E)
    end.

vhost({utf8, <<"vhost:", VHost/binary>>}) ->
    VHost;
vhost(_) ->
    application:get_env(rabbitmq_amqp1_0, default_vhost,
                        application:get_env(rabbit, default_vhost, <<"/">>)).

%% End 1-0

info(Pid, InfoItems) ->
    case InfoItems -- ?INFO_ITEMS of
        [] ->
            Ref = erlang:monitor(process, Pid),
            Pid ! {info, InfoItems, self()},
            receive
                {info_reply, Items} ->
                    erlang:demonitor(Ref),
                    Items;
                {'DOWN', _, process, Pid, _} ->
                    []
            end;
        UnknownItems -> throw({bad_argument, UnknownItems})
    end.

info_internal(node, #v1{}) -> node();
info_internal(auth_mechanism, #v1{connection = #v1_connection{auth_mechanism = none}}) ->
    none;
info_internal(auth_mechanism, #v1{connection = #v1_connection{auth_mechanism = {Name, _Mod}}}) ->
    Name;
info_internal(host, #v1{connection = #v1_connection{hostname = {utf8, Val}}}) ->
    Val;
info_internal(host, #v1{connection = #v1_connection{hostname = Val}}) ->
    Val;
info_internal(frame_max, #v1{connection = #v1_connection{frame_max = Val}}) ->
    Val;
info_internal(timeout, #v1{connection = #v1_connection{timeout_sec = Val}}) ->
    Val;
info_internal(user,
              #v1{connection = #v1_connection{user = #user{username = none}}}) ->
    '';
info_internal(username,
              #v1{connection = #v1_connection{user = #user{username = Val}}}) ->
    Val;
info_internal(state, #v1{connection_state = Val}) ->
    Val;
info_internal(SockStat, S) when SockStat =:= recv_oct;
                                SockStat =:= recv_cnt;
                                SockStat =:= send_oct;
                                SockStat =:= send_cnt;
                                SockStat =:= send_pend ->
    socket_info(fun (Sock) -> rabbit_net:getstat(Sock, [SockStat]) end,
                fun ([{_, I}]) -> I end, S);
info_internal(ssl, #v1{sock = Sock}) -> rabbit_net:is_ssl(Sock);
info_internal(ssl_protocol, S) -> ssl_info(fun ({P, _}) -> P end, S);
info_internal(ssl_key_exchange, S) -> ssl_info(fun ({_, {K, _, _}}) -> K end, S);
info_internal(ssl_cipher, S) -> ssl_info(fun ({_, {_, C, _}}) -> C end, S);
info_internal(ssl_hash, S) -> ssl_info(fun ({_, {_, _, H}}) -> H end, S);
info_internal(peer_cert_issuer, S) ->
    cert_info(fun rabbit_ssl:peer_cert_issuer/1, S);
info_internal(peer_cert_subject, S) ->
    cert_info(fun rabbit_ssl:peer_cert_subject/1, S);
info_internal(peer_cert_validity, S) ->
    cert_info(fun rabbit_ssl:peer_cert_validity/1, S).

%% From rabbit_reader
socket_info(Get, Select, #v1{sock = Sock}) ->
    case Get(Sock) of
        {ok,    T} -> Select(T);
        {error, _} -> ''
    end.

ssl_info(F, #v1{sock = Sock, proxy_socket = ProxySock}) ->
    case rabbit_net:proxy_ssl_info(Sock, ProxySock) of
        nossl       -> '';
        {error, _}  -> '';
        {ok, Items} ->
            P = proplists:get_value(protocol, Items),
            #{cipher := C,
              key_exchange := K,
              mac := H} = proplists:get_value(selected_cipher_suite, Items),
            F({P, {K, C, H}})
    end.

cert_info(F, #v1{sock = Sock}) ->
    case rabbit_net:peercert(Sock) of
        nossl      -> '';
        {error, _} -> '';
        {ok, Cert} -> list_to_binary(F(Cert))
    end.
