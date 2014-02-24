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

-module(rabbit_reader).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start_link/1, info_keys/0, info/1, info/2, force_event_refresh/1,
         shutdown/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/2, mainloop/2, recvloop/2]).

-export([conserve_resources/3, server_properties/1]).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 30).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).
-define(SILENT_CLOSE_DELAY, 3).

%%--------------------------------------------------------------------------

-record(v1, {parent, sock, connection, callback, recv_len, pending_recv,
             connection_state, helper_sup, queue_collector, heartbeater,
             stats_timer, channel_sup_sup_pid, buf, buf_len, throttle}).

-record(connection, {name, host, peer_host, port, peer_port,
                     protocol, user, timeout_sec, frame_max, vhost,
                     client_properties, capabilities,
                     auth_mechanism, auth_state}).

-record(throttle, {alarmed_by, last_blocked_by, last_blocked_at}).

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

-define(IS_STOPPING(State),
        (State#v1.connection_state =:= closing orelse
         State#v1.connection_state =:= closed)).

%%--------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (pid()) -> rabbit_types:ok(pid())).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (pid()) -> rabbit_types:infos()).
-spec(info/2 :: (pid(), rabbit_types:info_keys()) -> rabbit_types:infos()).
-spec(force_event_refresh/1 :: (pid()) -> 'ok').
-spec(shutdown/2 :: (pid(), string()) -> 'ok').
-spec(conserve_resources/3 :: (pid(), atom(), boolean()) -> 'ok').
-spec(server_properties/1 :: (rabbit_types:protocol()) ->
                                  rabbit_framing:amqp_table()).

%% These specs only exists to add no_return() to keep dialyzer happy
-spec(init/2 :: (pid(), pid()) -> no_return()).
-spec(start_connection/5 ::
        (pid(), pid(), any(), rabbit_net:socket(),
         fun ((rabbit_net:socket()) ->
                     rabbit_types:ok_or_error2(
                       rabbit_net:socket(), any()))) -> no_return()).

-spec(mainloop/2 :: (_,#v1{}) -> any()).
-spec(system_code_change/4 :: (_,_,_,_) -> {'ok',_}).
-spec(system_continue/3 :: (_,_,#v1{}) -> any()).
-spec(system_terminate/4 :: (_,_,_,_) -> none()).

-endif.

%%--------------------------------------------------------------------------

start_link(HelperSup) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [self(), HelperSup])}.

shutdown(Pid, Explanation) ->
    gen_server:call(Pid, {shutdown, Explanation}, infinity).

init(Parent, HelperSup) ->
    Deb = sys:debug_options([]),
    receive
        {go, Sock, SockTransform} ->
            start_connection(Parent, HelperSup, Deb, Sock, SockTransform)
    end.

system_continue(Parent, Deb, State) ->
    mainloop(Deb, State#v1{parent = Parent}).

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

conserve_resources(Pid, Source, Conserve) ->
    Pid ! {conserve_resources, Source, Conserve},
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
    [{<<"publisher_confirms">>,           bool, true},
     {<<"exchange_exchange_bindings">>,   bool, true},
     {<<"basic.nack">>,                   bool, true},
     {<<"consumer_cancel_notify">>,       bool, true},
     {<<"connection.blocked">>,           bool, true},
     {<<"consumer_priorities">>,          bool, true},
     {<<"authentication_failure_close">>, bool, true}];
server_capabilities(_) ->
    [].

%%--------------------------------------------------------------------------

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

socket_error(Reason) ->
    log(error, "error on AMQP connection ~p: ~p (~s)~n",
        [self(), Reason, rabbit_misc:format_inet_error(Reason)]).

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

socket_op(Sock, Fun) ->
    case Fun(Sock) of
        {ok, Res}       -> Res;
        {error, Reason} -> socket_error(Reason),
                           %% NB: this is tcp socket, even in case of ssl
                           rabbit_net:fast_close(Sock),
                           exit(normal)
    end.

start_connection(Parent, HelperSup, Deb, Sock, SockTransform) ->
    process_flag(trap_exit, true),
    Name = case rabbit_net:connection_string(Sock, inbound) of
               {ok, Str}         -> Str;
               {error, enotconn} -> rabbit_net:fast_close(Sock),
                                    exit(normal);
               {error, Reason}   -> socket_error(Reason),
                                    rabbit_net:fast_close(Sock),
                                    exit(normal)
           end,
    log(info, "accepting AMQP connection ~p (~s)~n", [self(), Name]),
    ClientSock = socket_op(Sock, SockTransform),
    erlang:send_after(?HANDSHAKE_TIMEOUT * 1000, self(), handshake_timeout),
    {PeerHost, PeerPort, Host, Port} =
        socket_op(Sock, fun (S) -> rabbit_net:socket_ends(S, inbound) end),
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
                queue_collector     = undefined,  %% started on tune-ok
                helper_sup          = HelperSup,
                heartbeater         = none,
                channel_sup_sup_pid = none,
                buf                 = [],
                buf_len             = 0,
                throttle            = #throttle{
                                         alarmed_by      = [],
                                         last_blocked_by = none,
                                         last_blocked_at = never}},
    try
        run({?MODULE, recvloop,
             [Deb, switch_callback(rabbit_event:init_stats_timer(
                                     State, #v1.stats_timer),
                                   handshake, 8)]}),
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
        rabbit_networking:unregister_connection(self()),
        rabbit_event:notify(connection_closed, [{pid, self()}])
    end,
    done.

run({M, F, A}) ->
    try apply(M, F, A)
    catch {become, MFA} -> run(MFA)
    end.

recvloop(Deb, State = #v1{pending_recv = true}) ->
    mainloop(Deb, State);
recvloop(Deb, State = #v1{connection_state = blocked}) ->
    mainloop(Deb, State);
recvloop(Deb, State = #v1{sock = Sock, recv_len = RecvLen, buf_len = BufLen})
  when BufLen < RecvLen ->
    case rabbit_net:setopts(Sock, [{active, once}]) of
        ok              -> mainloop(Deb, State#v1{pending_recv = true});
        {error, Reason} -> stop(Reason, State)
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
            stop(closed, State);
        {error, Reason} ->
            stop(Reason, State);
        {other, {system, From, Request}} ->
            sys:handle_system_msg(Request, From, State#v1.parent,
                                  ?MODULE, Deb, State);
        {other, Other}  ->
            case handle_other(Other, State) of
                stop     -> ok;
                NewState -> recvloop(Deb, NewState)
            end
    end.

stop(closed, State) -> maybe_emit_stats(State),
                       throw(connection_closed_abruptly);
stop(Reason, State) -> maybe_emit_stats(State),
                       throw({inet_error, Reason}).

handle_other({conserve_resources, Source, Conserve},
             State = #v1{throttle = Throttle = #throttle{alarmed_by = CR}}) ->
    CR1 = case Conserve of
              true  -> lists:usort([Source | CR]);
              false -> CR -- [Source]
          end,
    State1 = control_throttle(
               State#v1{throttle = Throttle#throttle{alarmed_by = CR1}}),
    case {blocked_by_alarm(State), blocked_by_alarm(State1)} of
        {false, true} -> ok = send_blocked(State1);
        {true, false} -> ok = send_unblocked(State1);
        {_,        _} -> ok
    end,
    State1;
handle_other({channel_closing, ChPid}, State) ->
    ok = rabbit_channel:ready_for_close(ChPid),
    channel_cleanup(ChPid),
    maybe_close(control_throttle(State));
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
    maybe_emit_stats(State),
    exit(Reason);
handle_other({channel_exit, _Channel, E = {writer, send_failed, _E}}, State) ->
    maybe_emit_stats(State),
    throw(E);
handle_other({channel_exit, Channel, Reason}, State) ->
    handle_exception(State, Channel, Reason);
handle_other({'DOWN', _MRef, process, ChPid, Reason}, State) ->
    handle_dependent_exit(ChPid, Reason, State);
handle_other(terminate_connection, State) ->
    maybe_emit_stats(State),
    stop;
handle_other(handshake_timeout, State)
  when ?IS_RUNNING(State) orelse ?IS_STOPPING(State) ->
    State;
handle_other(handshake_timeout, State) ->
    maybe_emit_stats(State),
    throw({handshake_timeout, State#v1.callback});
handle_other(heartbeat_timeout, State = #v1{connection_state = closed}) ->
    State;
handle_other(heartbeat_timeout, State = #v1{connection_state = S}) ->
    maybe_emit_stats(State),
    throw({heartbeat_timeout, S});
handle_other({'$gen_call', From, {shutdown, Explanation}}, State) ->
    {ForceTermination, NewState} = terminate(Explanation, State),
    gen_server:reply(From, ok),
    case ForceTermination of
        force  -> stop;
        normal -> NewState
    end;
handle_other({'$gen_call', From, info}, State) ->
    gen_server:reply(From, infos(?INFO_KEYS, State)),
    State;
handle_other({'$gen_call', From, {info, Items}}, State) ->
    gen_server:reply(From, try {ok, infos(Items, State)}
                           catch Error -> {error, Error}
                           end),
    State;
handle_other({'$gen_cast', force_event_refresh}, State)
  when ?IS_RUNNING(State) ->
    rabbit_event:notify(connection_created,
                        [{type, network} | infos(?CREATION_EVENT_KEYS, State)]),
    State;
handle_other({'$gen_cast', force_event_refresh}, State) ->
    %% Ignore, we will emit a created event once we start running.
    State;
handle_other(ensure_stats, State) ->
    ensure_stats_timer(State);
handle_other(emit_stats, State) ->
    emit_stats(State);
handle_other({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    control_throttle(State);
handle_other(Other, State) ->
    %% internal error -> something worth dying for
    maybe_emit_stats(State),
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

maybe_block(State = #v1{connection_state = blocking,
                        throttle         = Throttle}) ->
    ok = rabbit_heartbeat:pause_monitor(State#v1.heartbeater),
    State1 = State#v1{connection_state = blocked,
                      throttle = update_last_blocked_by(
                                   Throttle#throttle{
                                     last_blocked_at = erlang:now()})},
    case {blocked_by_alarm(State), blocked_by_alarm(State1)} of
        {false, true} -> ok = send_blocked(State1);
        {_,        _} -> ok
    end,
    State1;
maybe_block(State) ->
    State.


blocked_by_alarm(#v1{connection_state = blocked,
                     throttle         = #throttle{alarmed_by = CR}})
  when CR =/= [] ->
    true;
blocked_by_alarm(#v1{}) ->
    false.

send_blocked(#v1{throttle   = #throttle{alarmed_by = CR},
                 connection = #connection{protocol     = Protocol,
                                          capabilities = Capabilities},
                 sock       = Sock}) ->
    case rabbit_misc:table_lookup(Capabilities, <<"connection.blocked">>) of
        {bool, true} ->
            RStr = string:join([atom_to_list(A) || A <- CR], " & "),
            Reason = list_to_binary(rabbit_misc:format("low on ~s", [RStr])),
            ok = send_on_channel0(Sock, #'connection.blocked'{reason = Reason},
                                  Protocol);
        _ ->
            ok
    end.

send_unblocked(#v1{connection = #connection{protocol     = Protocol,
                                            capabilities = Capabilities},
                   sock       = Sock}) ->
    case rabbit_misc:table_lookup(Capabilities, <<"connection.blocked">>) of
        {bool, true} ->
            ok = send_on_channel0(Sock, #'connection.unblocked'{}, Protocol);
        _ ->
            ok
    end.

update_last_blocked_by(Throttle = #throttle{alarmed_by = []}) ->
    Throttle#throttle{last_blocked_by = flow};
update_last_blocked_by(Throttle) ->
    Throttle#throttle{last_blocked_by = resource}.

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
        {undefined,   controlled} -> State;
        {undefined, uncontrolled} -> exit({abnormal_dependent_exit,
                                           ChPid, Reason});
        {_Channel,    controlled} -> maybe_close(control_throttle(State));
        {Channel,   uncontrolled} -> State1 = handle_exception(
                                                State, Channel, Reason),
                                     maybe_close(control_throttle(State1))
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
            ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
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
             State = #v1{connection = #connection{protocol = Protocol}})
  when ?IS_STOPPING(State) ->
    case rabbit_command_assembler:analyze_frame(Type, Payload, Protocol) of
        {method, MethodName, FieldsBin} ->
            handle_method0(MethodName, FieldsBin, State);
        _Other -> State
    end;
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
handle_frame(_Type, _Channel, _Payload, State) when ?IS_STOPPING(State) ->
    State;
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
    %% This is not strictly necessary, but more obviously
    %% correct. Also note that we do not need to call maybe_close/1
    %% since we cannot possibly be in the 'closing' state.
    control_throttle(State);
post_process_frame({content_header, _, _, _, _}, _ChPid, State) ->
    maybe_block(State);
post_process_frame({content_body, _}, _ChPid, State) ->
    maybe_block(State);
post_process_frame(_Frame, _ChPid, State) ->
    State.

%%--------------------------------------------------------------------------

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

handle_input(handshake, <<"AMQP", A, B, C, D>>, State) ->
    handshake({A, B, C, D}, State);
handle_input(handshake, Other, #v1{sock = Sock}) ->
    refuse_connection(Sock, {bad_header, Other});

handle_input(Callback, Data, _State) ->
    throw({bad_input, Callback, Data}).

%% The two rules pertaining to version negotiation:
%%
%% * If the server cannot support the protocol specified in the
%% protocol header, it MUST respond with a valid protocol header and
%% then close the socket connection.
%%
%% * The server MUST provide a protocol version that is lower than or
%% equal to that requested by the client in the protocol header.
handshake({0, 0, 9, 1}, State) ->
    start_connection({0, 9, 1}, rabbit_framing_amqp_0_9_1, State);

%% This is the protocol header for 0-9, which we can safely treat as
%% though it were 0-9-1.
handshake({1, 1, 0, 9}, State) ->
    start_connection({0, 9, 0}, rabbit_framing_amqp_0_9_1, State);

%% This is what most clients send for 0-8.  The 0-8 spec, confusingly,
%% defines the version as 8-0.
handshake({1, 1, 8, 0}, State) ->
    start_connection({8, 0, 0}, rabbit_framing_amqp_0_8, State);

%% The 0-8 spec as on the AMQP web site actually has this as the
%% protocol header; some libraries e.g., py-amqplib, send it when they
%% want 0-8.
handshake({1, 1, 9, 1}, State) ->
    start_connection({8, 0, 0}, rabbit_framing_amqp_0_8, State);

%% ... and finally, the 1.0 spec is crystal clear!
handshake({Id, 1, 0, 0}, State) ->
    become_1_0(Id, State);

handshake(Vsn, #v1{sock = Sock}) ->
    refuse_connection(Sock, {bad_version, Vsn}).

%% Offer a protocol version to the client.  Connection.start only
%% includes a major and minor version number, Luckily 0-9 and 0-9-1
%% are similar enough that clients will be happy with either.
start_connection({ProtocolMajor, ProtocolMinor, _ProtocolRevision},
                 Protocol,
                 State = #v1{sock = Sock, connection = Connection}) ->
    rabbit_networking:register_connection(self()),
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

refuse_connection(Sock, Exception, {A, B, C, D}) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, <<"AMQP",A,B,C,D>>) end),
    throw(Exception).

-ifdef(use_specs).
-spec(refuse_connection/2 :: (rabbit_net:socket(), any()) -> no_return()).
-endif.
refuse_connection(Sock, Exception) ->
    refuse_connection(Sock, Exception, {0, 0, 9, 1}).

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
    catch throw:{inet_error, closed} ->
            maybe_emit_stats(State),
            throw(connection_closed_abruptly);
          exit:#amqp_error{method = none} = Reason ->
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
                            auth_mechanism    = {Mechanism, AuthMechanism},
                            auth_state        = AuthMechanism:init(Sock)}},
    auth_phase(Response, State);

handle_method0(#'connection.secure_ok'{response = Response},
               State = #v1{connection_state = securing}) ->
    auth_phase(Response, State);

handle_method0(#'connection.tune_ok'{frame_max = FrameMax,
                                     heartbeat = ClientHeartbeat},
               State = #v1{connection_state = tuning,
                           connection = Connection,
                           helper_sup = SupPid,
                           sock = Sock}) ->
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
            {ok, Collector} =
                rabbit_connection_helper_sup:start_queue_collector(SupPid),
            Frame = rabbit_binary_generator:build_heartbeat_frame(),
            SendFun = fun() -> catch rabbit_net:send(Sock, Frame) end,
            Parent = self(),
            ReceiveFun = fun() -> Parent ! heartbeat_timeout end,
            Heartbeater =
                rabbit_heartbeat:start(SupPid, Sock, ClientHeartbeat,
                                       SendFun, ClientHeartbeat, ReceiveFun),
            State#v1{connection_state = opening,
                     connection = Connection#connection{
                                    timeout_sec = ClientHeartbeat,
                                    frame_max = FrameMax},
                     queue_collector = Collector,
                     heartbeater = Heartbeater}
    end;

handle_method0(#'connection.open'{virtual_host = VHostPath},
               State = #v1{connection_state = opening,
                           connection       = Connection = #connection{
                                                user = User,
                                                protocol = Protocol},
                           helper_sup       = SupPid,
                           sock             = Sock,
                           throttle         = Throttle}) ->
    ok = rabbit_access_control:check_vhost_access(User, VHostPath),
    NewConnection = Connection#connection{vhost = VHostPath},
    ok = send_on_channel0(Sock, #'connection.open_ok'{}, Protocol),
    Conserve = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    Throttle1 = Throttle#throttle{alarmed_by = Conserve},
    {ok, ChannelSupSupPid} =
        rabbit_connection_helper_sup:start_channel_sup_sup(SupPid),
    State1 = control_throttle(
               State#v1{connection_state    = running,
                        connection          = NewConnection,
                        channel_sup_sup_pid = ChannelSupSupPid,
                        throttle            = Throttle1}),
    rabbit_event:notify(connection_created,
                        [{type, network} |
                         infos(?CREATION_EVENT_KEYS, State1)]),
    maybe_emit_stats(State1),
    State1;
handle_method0(#'connection.close'{}, State) when ?IS_RUNNING(State) ->
    lists:foreach(fun rabbit_channel:shutdown/1, all_channels()),
    maybe_close(State#v1{connection_state = closing});
handle_method0(#'connection.close'{},
               State = #v1{connection = #connection{protocol = Protocol},
                           sock = Sock})
  when ?IS_STOPPING(State) ->
    %% We're already closed or closing, so we don't need to cleanup
    %% anything.
    ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
    State;
handle_method0(#'connection.close_ok'{},
               State = #v1{connection_state = closed}) ->
    self() ! terminate_connection,
    State;
handle_method0(_Method, State) when ?IS_STOPPING(State) ->
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
           State = #v1{connection = Connection =
                           #connection{protocol       = Protocol,
                                       capabilities   = Capabilities,
                                       auth_mechanism = {Name, AuthMechanism},
                                       auth_state     = AuthState},
                       sock = Sock}) ->
    case AuthMechanism:handle_response(Response, AuthState) of
        {refused, Msg, Args} ->
            AmqpError = rabbit_misc:amqp_error(
                          access_refused, "~s login refused: ~s",
                          [Name, io_lib:format(Msg, Args)], none),
            case rabbit_misc:table_lookup(Capabilities,
                                          <<"authentication_failure_close">>) of
                {bool, true} ->
                    SafeMsg = io_lib:format(
                                "Login was refused using authentication "
                                "mechanism ~s. For details see the broker "
                                "logfile.", [Name]),
                    AmqpError1 = AmqpError#amqp_error{explanation = SafeMsg},
                    {0, CloseMethod} = rabbit_binary_generator:map_exception(
                                         0, AmqpError1, Protocol),
                    ok = send_on_channel0(State#v1.sock, CloseMethod, Protocol);
                _ -> ok
            end,
            rabbit_misc:protocol_error(AmqpError);
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
ic(auth_mechanism,    #connection{auth_mechanism = none})  -> none;
ic(auth_mechanism,    #connection{auth_mechanism = {Name, _Mod}}) -> Name;
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

maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #v1.stats_timer,
                            fun() -> emit_stats(State) end).

emit_stats(State) ->
    Infos = infos(?STATISTICS_KEYS, State),
    rabbit_event:notify(connection_stats, Infos),
    State1 = rabbit_event:reset_stats_timer(State, #v1.stats_timer),
    %% If we emit an event which looks like we are in flow control, it's not a
    %% good idea for it to be our last even if we go idle. Keep emitting
    %% events, either we stay busy or we drop out of flow control.
    %% The 5 is to match the test in formatters.js:fmt_connection_state().
    %% This magic number will go away when bug 24829 is merged.
    case proplists:get_value(last_blocked_age, Infos) < 5 of
        true -> ensure_stats_timer(State1);
        _    -> State1
    end.

%% 1.0 stub
-ifdef(use_specs).
-spec(become_1_0/2 :: (non_neg_integer(), #v1{}) -> no_return()).
-endif.
become_1_0(Id, State = #v1{sock = Sock}) ->
    case code:is_loaded(rabbit_amqp1_0_reader) of
        false -> refuse_connection(Sock, amqp1_0_plugin_not_enabled);
        _     -> Mode = case Id of
                            0 -> amqp;
                            3 -> sasl;
                            _ -> refuse_connection(
                                   Sock, {unsupported_amqp1_0_protocol_id, Id},
                                   {3, 1, 0, 0})
                        end,
                 throw({become, {rabbit_amqp1_0_reader, init,
                                 [Mode, pack_for_1_0(State)]}})
    end.

pack_for_1_0(#v1{parent              = Parent,
                 sock                = Sock,
                 recv_len            = RecvLen,
                 pending_recv        = PendingRecv,
                 helper_sup          = SupPid,
                 buf                 = Buf,
                 buf_len             = BufLen}) ->
    {Parent, Sock, RecvLen, PendingRecv, SupPid, Buf, BufLen}.
