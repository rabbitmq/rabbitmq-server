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

-module(rabbit_amqp1_0_reader).

%% Begin 1-0
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("rabbit_amqp1_0.hrl").
%% End 1-0


-export([start_link/3, info_keys/0, info/1, info/2, force_event_refresh/1,
         shutdown/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/4, mainloop/2]).

-export([conserve_resources/3, server_properties/1]).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 30).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).
-define(SILENT_CLOSE_DELAY, 3).

%%--------------------------------------------------------------------------

-record(v1, {parent, sock, connection, callback, recv_len, pending_recv,
             connection_state, queue_collector, heartbeater, stats_timer,
             channel_sup_sup_pid, start_heartbeat_fun, buf, buf_len, throttle}).

-record(connection, {name, host, peer_host, port, peer_port,
                     protocol, user, timeout_sec, frame_max, vhost,
                     client_properties, capabilities,
                     auth_mechanism, auth_state}).

-record(throttle, {conserve_resources, last_blocked_by, last_blocked_at}).

-define(STATISTICS_KEYS, [pid, recv_oct, recv_cnt, send_oct, send_cnt,
                          send_pend, state, last_blocked_by, last_blocked_age,
                          channels]).

-define(CREATION_EVENT_KEYS,
        [pid, name, port, peer_port, host,
        peer_host, ssl, peer_cert_subject, peer_cert_issuer,
        peer_cert_validity, auth_mechanism, ssl_protocol,
        ssl_key_exchange, ssl_cipher, ssl_hash, protocol, user, vhost,
        timeout, frame_max, client_properties]).

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
-spec(conserve_resources/3 :: (pid(), atom(), boolean()) -> 'ok').
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

conserve_resources(Pid, _Source, Conserve) ->
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

%%--------------------------------------------------------------------------

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

socket_op(Sock, Fun) ->
    case Fun(Sock) of
        {ok, Res}       -> Res;
        {error, Reason} -> log(error, "error on AMQP connection ~p: ~p~n",
                               [self(), Reason]),
                           %% NB: this is tcp socket, even in case of ssl
                           rabbit_net:fast_close(Sock),
                           exit(normal)
    end.

name(Sock) ->
    socket_op(Sock, fun (S) -> rabbit_net:connection_string(S, inbound) end).

socket_ends(Sock) ->
    socket_op(Sock, fun (S) -> rabbit_net:socket_ends(S, inbound) end).

start_connection(Parent, ChannelSupSupPid, Collector, StartHeartbeatFun, Deb,
                 Sock, SockTransform) ->
    process_flag(trap_exit, true),
    Name = name(Sock),
    log(info, "accepting AMQP connection ~p (~s)~n", [self(), Name]),
    ClientSock = socket_op(Sock, SockTransform),
    erlang:send_after(?HANDSHAKE_TIMEOUT * 1000, self(), handshake_timeout),
    {PeerHost, PeerPort, Host, Port} = socket_ends(Sock),
    State = #v1{parent              = Parent,
                sock                = ClientSock,
                connection          = #connection{
                  name               = list_to_binary(Name),
                  host               = Host,
                  peer_host          = PeerHost,
                  port               = Port,
                  peer_port          = PeerPort,
                  protocol           = none,
                  user               = none,
                  timeout_sec        = ?HANDSHAKE_TIMEOUT,
                  frame_max          = ?FRAME_MIN_SIZE,
                  vhost              = none,
                  client_properties  = none,
                  capabilities       = [],
                  auth_mechanism     = none,
                  auth_state         = none},
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
                throttle            = #throttle{
                  conserve_resources = false,
                  last_blocked_by    = none,
                  last_blocked_at    = never}},
    try
        ok = inet_op(fun () -> rabbit_net:tune_buffer_size(ClientSock) end),
        recvloop(Deb, switch_callback(rabbit_event:init_stats_timer(
                                       State, #v1.stats_timer),
                                      handshake, 8)),
        log(info, "closing AMQP connection ~p (~s)~n", [self(), Name])
    catch
        Ex -> log(case Ex of
                      connection_closed_abruptly -> warning;
                      _                          -> error
                  end, "closing AMQP connection ~p (~s):~n~p~n",
                  [self(), Name, Ex])
    after
        %% We don't call gen_tcp:close/1 here since it waits for
        %% pending output to be sent, which results in unnecessary
        %% delays. We could just terminate - the reader is the
        %% controlling process and hence its termination will close
        %% the socket. However, to keep the file_handle_cache
        %% accounting as accurate as possible we ought to close the
        %% socket w/o delay before termination.
        rabbit_net:fast_close(ClientSock),
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

handle_other({conserve_resources, Conserve}, Deb,
             State = #v1{throttle = Throttle}) ->
    Throttle1 = Throttle#throttle{conserve_resources = Conserve},
    recvloop(Deb, control_throttle(State#v1{throttle = Throttle1}));
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
handle_other(heartbeat_timeout, Deb, State = #v1{connection_state = closed}) ->
    mainloop(Deb, State);
handle_other(heartbeat_timeout, _Deb, #v1{connection_state = S}) ->
    throw({heartbeat_timeout, S});
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
handle_other(ensure_stats, Deb, State) ->
    mainloop(Deb, ensure_stats_timer(State));
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
    {normal, handle_exception(State, 0,
                              rabbit_misc:amqp_error(
                                connection_forced, Explanation, [], none))};
terminate(_Explanation, State) ->
    {force, State}.

control_throttle(State = #v1{connection_state = CS, throttle = Throttle}) ->
    case {CS, (Throttle#throttle.conserve_resources orelse
               credit_flow:blocked())} of
        {running,   true} -> State#v1{connection_state = blocking};
        {blocking, false} -> State#v1{connection_state = running};
        {blocked,  false} -> ok = rabbit_heartbeat:resume_monitor(
                                    State#v1.heartbeater),
                             State#v1{connection_state = running};
        {blocked,   true} -> State#v1{throttle = update_last_blocked_by(
                                                   Throttle)};
        {_,            _} -> State
    end.

maybe_block(State = #v1{connection_state = blocking, throttle = Throttle}) ->
    ok = rabbit_heartbeat:pause_monitor(State#v1.heartbeater),
    State#v1{connection_state = blocked,
             throttle = update_last_blocked_by(
                          Throttle#throttle{last_blocked_at = erlang:now()})};
maybe_block(State) ->
    State.

update_last_blocked_by(Throttle = #throttle{conserve_resources = true}) ->
    Throttle#throttle{last_blocked_by = resource};
update_last_blocked_by(Throttle = #throttle{conserve_resources = false}) ->
    Throttle#throttle{last_blocked_by = flow}.

%%--------------------------------------------------------------------------
%% error handling / termination

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
            maybe_close(handle_exception(control_throttle(State),
                                         Channel, Reason))
    end.

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

handle_exception(State = #v1{connection_state = closed}, Channel, Reason) ->
    log(error, "AMQP connection ~p (~p), channel ~p - error:~n~p~n",
        [self(), closed, Channel, Reason]),
    State;
handle_exception(State = #v1{connection = #connection{protocol = Protocol},
                             connection_state = CS},
                 Channel, Reason)
  when ?IS_RUNNING(State) orelse CS =:= closing ->
    log(error, "AMQP connection ~p (~p), channel ~p - error:~n~p~n",
        [self(), CS, Channel, Reason]),
    {0, CloseMethod} =
        rabbit_binary_generator:map_exception(Channel, Reason, Protocol),
    terminate_channels(),
    State1 = close_connection(State),
    ok = send_on_channel0(State1#v1.sock, CloseMethod, Protocol),
    State1;
handle_exception(State, Channel, Reason) ->
    %% We don't trust the client at this point - force them to wait
    %% for a bit so they can't DOS us with repeated failed logins etc.
    timer:sleep(?SILENT_CLOSE_DELAY * 1000),
    throw({handshake_error, State#v1.connection_state, Channel, Reason}).

%% we've "lost sync" with the client and hence must not accept any
%% more input
fatal_frame_error(Error, Type, Channel, Payload, State) ->
    frame_error(Error, Type, Channel, Payload, State),
    %% grace period to allow transmission of error
    timer:sleep(?SILENT_CLOSE_DELAY * 1000),
    throw(fatal_frame_error).

frame_error(Error, Type, Channel, Payload, State) ->
    {Str, Bin} = payload_snippet(Payload),
    handle_exception(State, Channel,
                     rabbit_misc:amqp_error(frame_error,
                                            "type ~p, ~s octets = ~p: ~p",
                                            [Type, Str, Bin, Error], none)).

unexpected_frame(Type, Channel, Payload, State) ->
    {Str, Bin} = payload_snippet(Payload),
    handle_exception(State, Channel,
                     rabbit_misc:amqp_error(unexpected_frame,
                                            "type ~p, ~s octets = ~p",
                                            [Type, Str, Bin], none)).

payload_snippet(Payload) when size(Payload) =< 16 ->
    {"all", Payload};
payload_snippet(<<Snippet:16/binary, _/binary>>) ->
    {"first 16", Snippet}.

%%--------------------------------------------------------------------------

create_channel(Channel, State) ->
    #v1{sock = Sock, queue_collector = Collector,
        channel_sup_sup_pid = ChanSupSup,
        connection = #connection{name         = Name,
                                 protocol     = Protocol,
                                 frame_max    = FrameMax,
                                 user         = User,
                                 vhost        = VHost,
                                 capabilities = Capabilities}} = State,
    {ok, _ChSupPid, {ChPid, AState}} =
        rabbit_channel_sup_sup:start_channel(
          ChanSupSup, {tcp, Sock, Channel, FrameMax, self(), Name,
                       Protocol, User, VHost, Capabilities, Collector}),
    MRef = erlang:monitor(process, ChPid),
    put({ch_pid, ChPid}, {Channel, MRef}),
    put({channel, Channel}, {ChPid, AState}),
    {ChPid, AState}.

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

%%--------------------------------------------------------------------------

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
        error     -> frame_error(unknown_frame, Type, 0, Payload, State);
        heartbeat -> State;
        {method, MethodName, FieldsBin} ->
            handle_method0(MethodName, FieldsBin, State);
        _Other    -> unexpected_frame(Type, 0, Payload, State)
    end;
handle_frame(Type, Channel, Payload,
             State = #v1{connection = #connection{protocol = Protocol}})
  when ?IS_RUNNING(State) ->
    case rabbit_command_assembler:analyze_frame(Type, Payload, Protocol) of
        error     -> frame_error(unknown_frame, Type, Channel, Payload, State);
        heartbeat -> unexpected_frame(Type, Channel, Payload, State);
        Frame     -> process_frame(Frame, Channel, State)
    end;
handle_frame(Type, Channel, Payload, State) ->
    unexpected_frame(Type, Channel, Payload, State).

process_frame(Frame, Channel, State) ->
    ChKey = {channel, Channel},
    {ChPid, AState} = case get(ChKey) of
                          undefined -> create_channel(Channel, State);
                          Other     -> Other
                      end,
    case rabbit_command_assembler:process(Frame, AState) of
        {ok, NewAState} ->
            put(ChKey, {ChPid, NewAState}),
            post_process_frame(Frame, ChPid, State);
        {ok, Method, NewAState} ->
            rabbit_channel:do(ChPid, Method),
            put(ChKey, {ChPid, NewAState}),
            post_process_frame(Frame, ChPid, State);
        {ok, Method, Content, NewAState} ->
            rabbit_channel:do_flow(ChPid, Method, Content),
            put(ChKey, {ChPid, NewAState}),
            post_process_frame(Frame, ChPid, control_throttle(State));
        {error, Reason} ->
            handle_exception(State, Channel, Reason)
    end.

post_process_frame({method, 'channel.close_ok', _}, ChPid, State) ->
    channel_cleanup(ChPid),
    State;
post_process_frame({content_header, _, _, _, _}, _ChPid, State) ->
    maybe_block(State);
post_process_frame({content_body, _}, _ChPid, State) ->
    maybe_block(State);
post_process_frame(_Frame, _ChPid, State) ->
    State.

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
        {sasl, false} -> handle_1_0_sasl_frame(Sections, State)
    end.

parse_1_0_frame(Payload) ->
    {PerfDesc, Rest} = rabbit_amqp1_0_binary_parser:parse(Payload),
    Perf = rabbit_amqp1_0_framing:decode(PerfDesc),
    Sections = case Rest of
                   <<>> -> Perf;
                   _    -> {Perf, Rest}
               end,
    ?DEBUG("1.0 frame decoded: ~p~n", [Sections]),
    Sections.

handle_1_0_connection_frame(#'v1_0.open'{ max_frame_size = ClientFrameMax,
                                          channel_max = ClientChannelMax,
                                          %% TODO idle_time_out
                                          hostname = _Hostname,
                                          properties = Props },
                            State = #v1{
                              start_heartbeat_fun = SHF,
                              connection_state = starting,
                              connection = Connection,
                              throttle   = Throttle,
                              sock = Sock}) ->
    Interval = undefined, %% TODO does 1-0 really no longer have heartbeating?
    ClientProps = case Props of
                      undefined -> [];
                      {map, Ps} -> Ps
                  end,
    ClientHeartbeat = case Interval of
                          undefined -> 0;
                          {_, HB} -> HB
                      end,
    FrameMax = case ClientFrameMax of
                   undefined -> unlimited;
                   {_, FM} -> FM
               end,
    ChannelMax = case ClientChannelMax of
                     undefined -> unlimited;
                     {_, CM} -> CM
                 end,
    State1 =
        if (FrameMax =/= unlimited) and (FrameMax < ?FRAME_1_0_MIN_SIZE) ->
                rabbit_misc:protocol_error(
                  not_allowed, "frame_max=~w < ~w min size",
                  [FrameMax, ?FRAME_1_0_MIN_SIZE]);
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
    %% TODO enforce channel_max
    ok = send_on_channel0(
           Sock,
           #'v1_0.open'{channel_max = ClientChannelMax,
                        max_frame_size = ClientFrameMax,
                        container_id = {utf8, list_to_binary(atom_to_list(node()))}},
           rabbit_amqp1_0_framing),
    Conserve = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    State2 = control_throttle(
               State1#v1{throttle = Throttle#throttle{
                                      conserve_resources = Conserve}}),
    rabbit_event:notify(connection_created,
                        infos(?CREATION_EVENT_KEYS, State2)),
    rabbit_event:if_enabled(State2, #v1.stats_timer,
                            fun() -> emit_stats(State2) end),
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

handle_1_0_sasl_frame(#'v1_0.sasl_init'{mechanism        = {symbol, Mechanism},
                                        initial_response = {binary, Response},
                                        hostname         = _Hostname},
                      State0 = #v1{connection_state = starting,
                                   connection       = Connection,
                                   sock             = Sock}) ->
    AuthMechanism = auth_mechanism_to_module(list_to_binary(Mechanism), Sock),
    State = State0#v1{connection       =
                          Connection#connection{
                            auth_mechanism    = AuthMechanism,
                            auth_state        = AuthMechanism:init(Sock)},
                      connection_state = securing},
    auth_phase_1_0(Response, State);
handle_1_0_sasl_frame(#'v1_0.sasl_response'{response = {binary, Response}},
                      State = #v1{connection_state = securing}) ->
    auth_phase_1_0(Response, State);
handle_1_0_sasl_frame(Frame, State) ->
    throw({unexpected_1_0_sasl_frame, Frame, State}).

%% End 1-0

%% We allow clients to exceed the frame size a little bit since quite
%% a few get it wrong - off-by 1 or 8 (empty frame size) are typical.
-define(FRAME_SIZE_FUDGE, ?EMPTY_FRAME_SIZE).

handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32>>,
             State = #v1{connection = #connection{frame_max = FrameMax}})
  when FrameMax /= 0 andalso
       PayloadSize > FrameMax - ?EMPTY_FRAME_SIZE + ?FRAME_SIZE_FUDGE ->
    fatal_frame_error(
      {frame_too_large, PayloadSize, FrameMax - ?EMPTY_FRAME_SIZE},
      Type, Channel, <<>>, State);
handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32>>, State) ->
    ensure_stats_timer(
      switch_callback(State, {frame_payload, Type, Channel, PayloadSize},
                      PayloadSize + 1));

handle_input({frame_payload, Type, Channel, PayloadSize}, Data, State) ->
    <<Payload:PayloadSize/binary, EndMarker>> = Data,
    case EndMarker of
        ?FRAME_END -> State1 = handle_frame(Type, Channel, Payload, State),
                      switch_callback(State1, frame_header, 7);
        _          -> fatal_frame_error({invalid_frame_end_marker, EndMarker},
                                        Type, Channel, Payload, State)
    end;

%% Begin 1-0

handle_input({frame_header_1_0, Mode},
             Header = <<Size:32, DOff:8, Type:8, Channel:16>>,
             State) when DOff >= 2 ->
    ?DEBUG("1.0 frame header: doff: ~p size: ~p~n", [DOff, Size]),
    case {Mode, Type} of
        {amqp, 0} -> ok;
        {sasl, 1} -> ok;
        _         -> throw({bad_1_0_header_type, Header, Mode})
    end,
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
handle_input(handshake, <<"AMQP", 0, 1, 0, 0>>, State) ->
    start_1_0_connection(amqp, {1, 0, 0}, rabbit_amqp1_0_framing, State);

%% 3 stands for "SASL"
handle_input(handshake, <<"AMQP", 3, 1, 0, 0>>, State) ->
    start_1_0_connection(sasl, {1, 0, 0}, rabbit_amqp1_0_framing, State);

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

start_1_0_connection(sasl, {1, 0, 0}, Protocol, State = #v1{sock = Sock}) ->
    send_1_0_handshake(Sock, <<"AMQP",3,1,0,0>>),
    Ms = {array, symbol, [atom_to_list(M) || M <- auth_mechanisms(Sock)]},
    Mechanisms = #'v1_0.sasl_mechanisms'{sasl_server_mechanisms = Ms},
    ok = send_on_channel0(Sock, Mechanisms, rabbit_amqp1_0_sasl),
    start_1_0_connection0(sasl, Protocol, State);

start_1_0_connection(amqp, {1, 0, 0}, Protocol,
                     State = #v1{sock       = Sock,
                                 connection = C = #connection{user = User}}) ->
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
                               connection = C#connection{user = NoAuthUser}},
                    send_1_0_handshake(Sock, <<"AMQP",0,1,0,0>>),
                    start_1_0_connection0(amqp, Protocol, State1);
                _ ->
                    send_1_0_handshake(Sock, <<"AMQP",3,1,0,0>>),
                    throw(default_user_missing)
            end;
        _ ->
            send_1_0_handshake(Sock, <<"AMQP",0,1,0,0>>),
            start_1_0_connection0(amqp, Protocol, State)
    end.

start_1_0_connection0(Mode, Protocol, State = #v1{connection = Connection}) ->
    switch_callback(State#v1{connection = Connection#connection{
                                            timeout_sec = ?NORMAL_TIMEOUT,
                                            protocol = Protocol},
                             connection_state = starting},
                    {frame_header_1_0, Mode}, 8).

send_1_0_handshake(Sock, Handshake) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, Handshake) end).

%% End 1-0

%% TODO this is a 1-0 issue: if someone tries to connect with e.g. 0-10, what
%% should we tell them we support? The spec says the highest version we have
%% - which is 1-0. Is that TRTTD?
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
    try
        handle_method0(Protocol:decode_method_fields(MethodName, FieldsBin),
                       State)
    catch exit:#amqp_error{method = none} = Reason ->
            handle_exception(State, 0, Reason#amqp_error{method = MethodName});
          Type:Reason ->
            Stack = erlang:get_stacktrace(),
            handle_exception(State, 0, {Type, Reason, MethodName, Stack})
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
    State = State0#v1{connection_state = securing,
                      connection       =
                          Connection#connection{
                            client_properties = ClientProperties,
                            capabilities      = Capabilities,
                            auth_mechanism    = AuthMechanism,
                            auth_state        = AuthMechanism:init(Sock)}},
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
            ReceiveFun = fun() -> Parent ! heartbeat_timeout end,
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
                           connection       = Connection = #connection{
                                                user = User,
                                                protocol = Protocol},
                           sock             = Sock,
                           throttle         = Throttle}) ->
    ok = rabbit_access_control:check_vhost_access(User, VHostPath),
    NewConnection = Connection#connection{vhost = VHostPath},
    ok = send_on_channel0(Sock, #'connection.open_ok'{}, Protocol),
    Conserve = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    State1 = control_throttle(
               State#v1{connection_state   = running,
                        connection         = NewConnection,
                        throttle           = Throttle#throttle{
                                               conserve_resources = Conserve}}),
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

server_frame_max() ->
    {ok, FrameMax} = application:get_env(rabbit, frame_max),
    FrameMax.

server_heartbeat() ->
    {ok, Heartbeat} = application:get_env(rabbit, heartbeat),
    Heartbeat.

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
           State = #v1{connection = Connection =
                           #connection{protocol       = Protocol,
                                       auth_mechanism = AuthMechanism,
                                       auth_state     = AuthState},
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
            State#v1{connection = Connection#connection{
                                    auth_state = AuthState1}};
        {ok, User} ->
            Tune = #'connection.tune'{channel_max = 0,
                                      frame_max = server_frame_max(),
                                      heartbeat = server_heartbeat()},
            ok = send_on_channel0(Sock, Tune, Protocol),
            State#v1{connection_state = tuning,
                     connection = Connection#connection{user       = User,
                                                        auth_state = none}}
    end.

%% Begin 1-0

auth_phase_1_0(Response,
               State = #v1{connection = Connection =
                               #connection{protocol       = Protocol,
                                           auth_mechanism = AuthMechanism,
                                           auth_state     = AuthState},
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
            Secure = #'v1_0.sasl_challenge'{challenge = {binary, Challenge}},
            ok = send_on_channel0(Sock, Secure, rabbit_amqp1_0_sasl),
            State#v1{connection = Connection =
                         #connection{auth_state = AuthState1}};
        {ok, User} ->
            Outcome = #'v1_0.sasl_outcome'{code = {ubyte, 0}},
            ok = send_on_channel0(Sock, Outcome, rabbit_amqp1_0_sasl),
            switch_callback(
              State#v1{connection_state = waiting_amqp0100,
                       connection = Connection#connection{user = User}},
              handshake, 8)
    end.

%% End 1-0

%%--------------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(pid,                #v1{}) -> self();
i(SockStat,           S) when SockStat =:= recv_oct;
                              SockStat =:= recv_cnt;
                              SockStat =:= send_oct;
                              SockStat =:= send_cnt;
                              SockStat =:= send_pend ->
    socket_info(fun (Sock) -> rabbit_net:getstat(Sock, [SockStat]) end,
                fun ([{_, I}]) -> I end, S);
i(ssl,                #v1{sock = Sock}) -> rabbit_net:is_ssl(Sock);
i(ssl_protocol,       S) -> ssl_info(fun ({P,         _}) -> P end, S);
i(ssl_key_exchange,   S) -> ssl_info(fun ({_, {K, _, _}}) -> K end, S);
i(ssl_cipher,         S) -> ssl_info(fun ({_, {_, C, _}}) -> C end, S);
i(ssl_hash,           S) -> ssl_info(fun ({_, {_, _, H}}) -> H end, S);
i(peer_cert_issuer,   S) -> cert_info(fun rabbit_ssl:peer_cert_issuer/1,   S);
i(peer_cert_subject,  S) -> cert_info(fun rabbit_ssl:peer_cert_subject/1,  S);
i(peer_cert_validity, S) -> cert_info(fun rabbit_ssl:peer_cert_validity/1, S);
i(state,              #v1{connection_state = CS}) -> CS;
i(last_blocked_by,    #v1{throttle = #throttle{last_blocked_by = By}}) -> By;
i(last_blocked_age,   #v1{throttle = #throttle{last_blocked_at = never}}) ->
    infinity;
i(last_blocked_age,   #v1{throttle = #throttle{last_blocked_at = T}}) ->
    timer:now_diff(erlang:now(), T) / 1000000;
i(channels,           #v1{}) -> length(all_channels());
i(Item,               #v1{connection = Conn}) -> ic(Item, Conn).

ic(name,              #connection{name        = Name})     -> Name;
ic(host,              #connection{host        = Host})     -> Host;
ic(peer_host,         #connection{peer_host   = PeerHost}) -> PeerHost;
ic(port,              #connection{port        = Port})     -> Port;
ic(peer_port,         #connection{peer_port   = PeerPort}) -> PeerPort;
ic(protocol,          #connection{protocol    = none})     -> none;
ic(protocol,          #connection{protocol    = P})        -> P:version();
ic(user,              #connection{user        = none})     -> '';
ic(user,              #connection{user        = U})        -> U#user.username;
ic(vhost,             #connection{vhost       = VHost})    -> VHost;
ic(timeout,           #connection{timeout_sec = Timeout})  -> Timeout;
ic(frame_max,         #connection{frame_max   = FrameMax}) -> FrameMax;
ic(client_properties, #connection{client_properties = CP}) -> CP;
ic(auth_mechanism,    #connection{auth_mechanism = none}) ->
    none;
ic(auth_mechanism,    #connection{auth_mechanism = Mechanism}) ->
    proplists:get_value(name, Mechanism:description());
ic(Item,              #connection{}) -> throw({bad_argument, Item}).

socket_info(Get, Select, #v1{sock = Sock}) ->
    case Get(Sock) of
        {ok,    T} -> Select(T);
        {error, _} -> ''
    end.

ssl_info(F, #v1{sock = Sock}) ->
    %% The first ok form is R14
    %% The second is R13 - the extra term is exportability (by inspection,
    %% the docs are wrong)
    case rabbit_net:ssl_info(Sock) of
        nossl                   -> '';
        {error, _}              -> '';
        {ok, {P, {K, C, H}}}    -> F({P, {K, C, H}});
        {ok, {P, {K, C, H, _}}} -> F({P, {K, C, H}})
    end.

cert_info(F, #v1{sock = Sock}) ->
    case rabbit_net:peercert(Sock) of
        nossl                -> '';
        {error, no_peercert} -> '';
        {ok, Cert}           -> list_to_binary(F(Cert))
    end.

emit_stats(State) ->
    rabbit_event:notify(connection_stats, infos(?STATISTICS_KEYS, State)),
    rabbit_event:reset_stats_timer(State, #v1.stats_timer).

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
          %% NB subtract fixed frame header size
          ChanSupSup, {Protocol, Sock, Channel,
                       case FrameMax of
                           unlimited -> unlimited;
                           _         -> FrameMax - 8
                       end,
                       self(), User, VHost, Collector}),
    erlang:monitor(process, ChFrPid),
    put({channel, Channel}, {ch_fr_pid, ChFrPid}),
    put({ch_sup_pid, ChSupPid}, {{channel, Channel}, {ch_fr_pid, ChFrPid}}),
    put({ch_fr_pid, ChFrPid}, {channel, Channel}),
    ok = rabbit_amqp1_0_session:process_frame(ChFrPid, Frame).

%% End 1-0
