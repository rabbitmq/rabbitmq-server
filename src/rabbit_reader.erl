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

-module(rabbit_reader).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start_link/3, info_keys/0, info/1, info/2, force_event_refresh/1,
         shutdown/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/4, mainloop/2]).

-export([conserve_resources/2, server_properties/1]).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 30).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).
-define(SILENT_CLOSE_DELAY, 3).

%%--------------------------------------------------------------------------

-record(v1, {parent, sock, connection, callback, recv_len, pending_recv,
             connection_state, queue_collector, heartbeater, stats_timer,
             channel_sup_sup_pid, start_heartbeat_fun, buf, buf_len,
             auth_mechanism, auth_state, conserve_resources,
             last_blocked_by, last_blocked_at}).

-define(STATISTICS_KEYS, [pid, recv_oct, recv_cnt, send_oct, send_cnt,
                          send_pend, state, last_blocked_by, last_blocked_age,
                          channels]).

-define(CREATION_EVENT_KEYS, [pid, name, address, port, peer_address, peer_port,
                              ssl, peer_cert_subject, peer_cert_issuer,
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
-spec(force_event_refresh/1 :: (pid()) -> 'ok').
-spec(shutdown/2 :: (pid(), string()) -> 'ok').
-spec(conserve_resources/2 :: (pid(), boolean()) -> 'ok').
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

-spec(mainloop/2 :: (_,#v1{}) -> any()).
-spec(system_code_change/4 :: (_,_,_,_) -> {'ok',_}).
-spec(system_continue/3 :: (_,_,#v1{}) -> any()).
-spec(system_terminate/4 :: (_,_,_,_) -> none()).

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

force_event_refresh(Pid) ->
    gen_server:cast(Pid, force_event_refresh).

conserve_resources(Pid, Conserve) ->
    Pid ! {conserve_resources, Conserve},
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

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

socket_op(Sock, Fun) ->
    case Fun(Sock) of
        {ok, Res}       -> Res;
        {error, Reason} -> log(error, "error on AMQP connection ~p: ~p~n",
                               [self(), Reason]),
                           exit(normal)
    end.

name(Sock) ->
    socket_op(Sock, fun (S) -> rabbit_net:connection_string(S, inbound) end).

start_connection(Parent, ChannelSupSupPid, Collector, StartHeartbeatFun, Deb,
                 Sock, SockTransform) ->
    process_flag(trap_exit, true),
    ConnStr = name(Sock),
    log(info, "accepting AMQP connection ~p (~s)~n", [self(), ConnStr]),
    ClientSock = socket_op(Sock, SockTransform),
    erlang:send_after(?HANDSHAKE_TIMEOUT * 1000, self(),
                      handshake_timeout),
    State = #v1{parent              = Parent,
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
                channel_sup_sup_pid = ChannelSupSupPid,
                start_heartbeat_fun = StartHeartbeatFun,
                buf                 = [],
                buf_len             = 0,
                auth_mechanism      = none,
                auth_state          = none,
                conserve_resources  = false,
                last_blocked_by     = none,
                last_blocked_at     = never},
    try
        BufSizes = inet_op(fun () ->
                                   rabbit_net:getopts(
                                     ClientSock, [sndbuf, recbuf, buffer])
                           end),
        BufSz = lists:max([Sz || {_Opt, Sz} <- BufSizes]),
        ok = inet_op(fun () ->
                             rabbit_net:setopts(ClientSock, [{buffer, BufSz}])
                     end),
        recvloop(Deb, switch_callback(rabbit_event:init_stats_timer(
                                       State, #v1.stats_timer),
                                      handshake, 8)),
        log(info, "closing AMQP connection ~p (~s)~n", [self(), ConnStr])
    catch
        Ex -> log(case Ex of
                      connection_closed_abruptly -> warning;
                      _                          -> error
                  end, "closing AMQP connection ~p (~s):~n~p~n",
                  [self(), ConnStr, Ex])
    after
        %% The reader is the controlling process and hence its
        %% termination will close the socket. Furthermore,
        %% gen_tcp:close/1 waits for pending output to be sent, which
        %% results in unnecessary delays. However, to keep the
        %% file_handle_cache accounting as accurate as possible it
        %% would be good to close the socket immediately if we
        %% can. But we can only do this for non-ssl sockets.
        %%
        rabbit_net:maybe_fast_close(ClientSock),
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
        closed          -> case State#v1.connection_state of
                               closed -> State;
                               _      -> throw(connection_closed_abruptly)
                           end;
        {error, Reason} -> throw({inet_error, Reason});
        {other, Other}  -> handle_other(Other, Deb, State)
    end.

handle_other({conserve_resources, Conserve}, Deb, State) ->
    recvloop(Deb, control_throttle(State#v1{conserve_resources = Conserve}));
handle_other({channel_closing, ChPid}, Deb, State) ->
    ok = rabbit_channel:ready_for_close(ChPid),
    channel_cleanup(ChPid),
    mainloop(Deb, maybe_close(control_throttle(State)));
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
handle_other({'$gen_cast', force_event_refresh}, Deb, State)
  when ?IS_RUNNING(State) ->
    rabbit_event:notify(connection_created,
                        [{type, network} | infos(?CREATION_EVENT_KEYS, State)]),
    mainloop(Deb, State);
handle_other({'$gen_cast', force_event_refresh}, Deb, State) ->
    %% Ignore, we will emit a created event once we start running.
    mainloop(Deb, State);
handle_other(emit_stats, Deb, State) ->
    mainloop(Deb, emit_stats(State));
handle_other({system, From, Request}, Deb, State = #v1{parent = Parent}) ->
    sys:handle_system_msg(Request, From, Parent, ?MODULE, Deb, State);
handle_other({bump_credit, Msg}, Deb, State) ->
    credit_flow:handle_bump_msg(Msg),
    recvloop(Deb, control_throttle(State));
handle_other(Other, _Deb, _State) ->
    %% internal error -> something worth dying for
    exit({unexpected_message, Other}).

switch_callback(State, Callback, Length) ->
    State#v1{callback = Callback, recv_len = Length}.

terminate(Explanation, State) when ?IS_RUNNING(State) ->
    {normal, send_exception(State, 0,
                            rabbit_misc:amqp_error(
                              connection_forced, Explanation, [], none))};
terminate(_Explanation, State) ->
    {force, State}.

control_throttle(State = #v1{connection_state   = CS,
                             conserve_resources = Mem}) ->
    case {CS, Mem orelse credit_flow:blocked()} of
        {running,   true} -> State#v1{connection_state = blocking};
        {blocking, false} -> State#v1{connection_state = running};
        {blocked,  false} -> ok = rabbit_heartbeat:resume_monitor(
                                    State#v1.heartbeater),
                             State#v1{connection_state = running};
        {blocked,   true} -> update_last_blocked_by(State);
        {_,            _} -> State
    end.

maybe_block(State = #v1{connection_state = blocking}) ->
    ok = rabbit_heartbeat:pause_monitor(State#v1.heartbeater),
    update_last_blocked_by(State#v1{connection_state = blocked,
                                    last_blocked_at  = erlang:now()});
maybe_block(State) ->
    State.

update_last_blocked_by(State = #v1{conserve_resources = true}) ->
    State#v1{last_blocked_by = resource};
update_last_blocked_by(State = #v1{conserve_resources = false}) ->
    State#v1{last_blocked_by = flow}.

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
    erlang:send_after((if TimeoutSec > 0 andalso
                          TimeoutSec < ?CLOSING_TIMEOUT -> TimeoutSec;
                          true                          -> ?CLOSING_TIMEOUT
                       end) * 1000, self(), terminate_connection),
    State#v1{connection_state = closed}.

handle_dependent_exit(ChPid, Reason, State) ->
    case {channel_cleanup(ChPid), termination_kind(Reason)} of
        {undefined, uncontrolled} ->
            exit({abnormal_dependent_exit, ChPid, Reason});
        {_Channel, controlled} ->
            maybe_close(control_throttle(State));
        {Channel, uncontrolled} ->
            log(error, "AMQP connection ~p, channel ~p - error:~n~p~n",
                [self(), Channel, Reason]),
            maybe_close(handle_exception(control_throttle(State),
                                         Channel, Reason))
    end.

channel_cleanup(ChPid) ->
    case get({ch_pid, ChPid}) of
        undefined       -> undefined;
        {Channel, MRef} -> credit_flow:peer_down(ChPid),
                           erase({channel, Channel}),
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
            case {channel_cleanup(ChPid), termination_kind(Reason)} of
                {undefined, _} ->
                    exit({abnormal_dependent_exit, ChPid, Reason});
                {_Channel, controlled} ->
                    wait_for_channel_termination(N-1, TimerRef);
                {Channel, uncontrolled} ->
                    log(error,
                        "AMQP connection ~p, channel ~p - "
                        "error while terminating:~n~p~n",
                        [self(), Channel, Reason]),
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
            ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
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
        AnalyzedFrame -> process_frame(AnalyzedFrame, Channel, State)
    end.

process_frame(Frame, Channel, State) ->
    case get({channel, Channel}) of
        {ChPid, AState} ->
            case process_channel_frame(Frame,  ChPid, AState) of
                {ok, NewAState} -> put({channel, Channel}, {ChPid, NewAState}),
                                   post_process_frame(Frame, ChPid, State);
                {error, Reason} -> handle_exception(State, Channel, Reason)
            end;
        undefined when ?IS_RUNNING(State) ->
            ok = create_channel(Channel, State),
            process_frame(Frame, Channel, State);
        undefined ->
            throw({channel_frame_while_starting,
                   Channel, State#v1.connection_state, Frame})
    end.

post_process_frame({method, 'channel.close_ok', _}, ChPid, State) ->
    channel_cleanup(ChPid),
    control_throttle(State);
post_process_frame({method, MethodName, _}, _ChPid,
                   State = #v1{connection = #connection{
                                 protocol = Protocol}}) ->
    case Protocol:method_has_content(MethodName) of
        true  -> erlang:bump_reductions(2000),
                 maybe_block(control_throttle(State));
        false -> control_throttle(State)
    end;
post_process_frame(_Frame, _ChPid, State) ->
    control_throttle(State).

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

refuse_connection(Sock, Exception) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, <<"AMQP",0,0,9,1>>) end),
    throw(Exception).

ensure_stats_timer(State = #v1{connection_state = running}) ->
    rabbit_event:ensure_stats_timer(State, #v1.stats_timer, emit_stats);
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
                           sock = Sock}) ->
    ok = rabbit_access_control:check_vhost_access(User, VHostPath),
    NewConnection = Connection#connection{vhost = VHostPath},
    ok = send_on_channel0(Sock, #'connection.open_ok'{}, Protocol),
    Conserve = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    State1 = control_throttle(
               State#v1{connection_state   = running,
                        connection         = NewConnection,
                        conserve_resources = Conserve}),
    rabbit_event:notify(connection_created,
                        [{type, network} |
                         infos(?CREATION_EVENT_KEYS, State1)]),
    rabbit_event:if_enabled(State1, #v1.stats_timer,
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

send_on_channel0(Sock, Method, Protocol) ->
    ok = rabbit_writer:internal_send_command(Sock, 0, Method, Protocol).

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
i(name, #v1{sock = Sock}) ->
    list_to_binary(name(Sock));
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
i(last_blocked_by, #v1{last_blocked_by = By}) ->
    By;
i(last_blocked_age, #v1{last_blocked_at = never}) ->
    infinity;
i(last_blocked_age, #v1{last_blocked_at = T}) ->
    timer:now_diff(erlang:now(), T) / 1000000;
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

create_channel(Channel, State) ->
    #v1{sock = Sock, queue_collector = Collector,
        channel_sup_sup_pid = ChanSupSup,
        connection = #connection{protocol     = Protocol,
                                 frame_max    = FrameMax,
                                 user         = User,
                                 vhost        = VHost,
                                 capabilities = Capabilities}} = State,
    {ok, _ChSupPid, {ChPid, AState}} =
        rabbit_channel_sup_sup:start_channel(
          ChanSupSup, {tcp, Sock, Channel, FrameMax, self(), name(Sock),
                       Protocol, User, VHost, Capabilities, Collector}),
    MRef = erlang:monitor(process, ChPid),
    put({ch_pid, ChPid}, {Channel, MRef}),
    put({channel, Channel}, {ChPid, AState}),
    ok.

process_channel_frame(Frame, ChPid, AState) ->
    case rabbit_command_assembler:process(Frame, AState) of
        {ok, NewAState}                  -> {ok, NewAState};
        {ok, Method, NewAState}          -> rabbit_channel:do(ChPid, Method),
                                            {ok, NewAState};
        {ok, Method, Content, NewAState} -> rabbit_channel:do_flow(
                                              ChPid, Method, Content),
                                            {ok, NewAState};
        {error, Reason}                  -> {error, Reason}
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

emit_stats(State) ->
    rabbit_event:notify(connection_stats, infos(?STATISTICS_KEYS, State)),
    rabbit_event:reset_stats_timer(State, #v1.stats_timer).
