%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_reader).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

%% This is an AMQP 0-9-1 connection implementation. If AMQP 1.0 plugin is enabled,
%% this module passes control of incoming AMQP 1.0 connections to it.
%%
%% Every connection (as in, a process using this module)
%% is a controlling process for a server socket.
%%
%% Connections have a number of responsibilities:
%%
%%  * Performing protocol handshake
%%  * Parsing incoming data and dispatching protocol methods
%%  * Authenticating clients (with the help of authentication backends)
%%  * Enforcing TCP backpressure (throttling clients)
%%  * Enforcing connection limits, e.g. channel_max
%%  * Channel management
%%  * Setting up heartbeater and alarm notifications
%%  * Emitting connection and network activity metric events
%%  * Gracefully handling client disconnects, channel termination, etc
%%
%% and a few more.
%%
%% Every connection has
%%
%%  * a queue collector which is responsible for keeping
%%    track of exclusive queues on the connection and their cleanup.
%%  * a heartbeater that's responsible for sending heartbeat frames to clients,
%%    keeping track of the incoming ones and notifying connection about
%%    heartbeat timeouts
%%  * Stats timer, a timer that is used to periodically emit metric events
%%
%% Some dependencies are started under a separate supervisor to avoid deadlocks
%% during system shutdown. See rabbit_channel_sup:start_link/0 for details.
%%
%% Reader processes are special processes (in the OTP sense).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/2, info_keys/0, info/1, info/2, force_event_refresh/2,
         shutdown/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/3, mainloop/4, recvloop/4]).

-export([conserve_resources/3, server_properties/1]).

-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 30).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).
%% we wait for this many seconds before closing TCP connection
%% with a client that failed to log in. Provides some relief
%% from connection storms and DoS.
-define(SILENT_CLOSE_DELAY, 3).
-define(CHANNEL_MIN, 1).

%%--------------------------------------------------------------------------

-record(v1, {
          %% parent process
          parent,
          %% socket
          sock,
          %% connection state, see connection record
          connection,
          callback,
          recv_len,
          pending_recv,
          %% pre_init | securing | running | blocking | blocked | closing | closed | {become, F}
          connection_state,
          %% see comment in rabbit_connection_sup:start_link/0
          helper_sup,
          %% takes care of cleaning up exclusive queues,
          %% see rabbit_queue_collector
          queue_collector,
          %% sends and receives heartbeat frames,
          %% see rabbit_heartbeat
          heartbeater,
          %% timer used to emit statistics
          stats_timer,
          %% channel supervisor
          channel_sup_sup_pid,
          %% how many channels this connection has
          channel_count,
          %% throttling state, for both
          %% credit- and resource-driven flow control
          throttle,
          proxy_socket}).

-record(throttle, {
  %% never | timestamp()
  last_blocked_at,
  %% a set of the reasons why we are
  %% blocked: {resource, memory}, {resource, disk}.
  %% More reasons can be added in the future.
  blocked_by,
  %% true if received any publishes, false otherwise
  %% note that this will also be true when connection is
  %% already blocked
  should_block,
  %% true if we had we sent a connection.blocked,
  %% false otherwise
  connection_blocked_message_sent
}).

-define(STATISTICS_KEYS, [pid, recv_oct, recv_cnt, send_oct, send_cnt,
                          send_pend, state, channels, reductions,
                          garbage_collection]).

-define(SIMPLE_METRICS, [pid, recv_oct, send_oct, reductions]).
-define(OTHER_METRICS, [recv_cnt, send_cnt, send_pend, state, channels,
                        garbage_collection]).

-define(CREATION_EVENT_KEYS,
        [pid, name, port, peer_port, host,
        peer_host, ssl, peer_cert_subject, peer_cert_issuer,
        peer_cert_validity, auth_mechanism, ssl_protocol,
        ssl_key_exchange, ssl_cipher, ssl_hash, protocol, user, vhost,
        timeout, frame_max, channel_max, client_properties, connected_at,
        node, user_who_performed_action]).

-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

-define(AUTH_NOTIFICATION_INFO_KEYS,
        [host, name, peer_host, peer_port, protocol, auth_mechanism,
         ssl, ssl_protocol, ssl_cipher, peer_cert_issuer, peer_cert_subject,
         peer_cert_validity]).

-define(IS_RUNNING(State),
        (State#v1.connection_state =:= running orelse
         State#v1.connection_state =:= blocked)).

-define(IS_STOPPING(State),
        (State#v1.connection_state =:= closing orelse
         State#v1.connection_state =:= closed)).

%%--------------------------------------------------------------------------

-type resource_alert() :: {WasAlarmSetForNode :: boolean(),
                           IsThereAnyAlarmsWithSameSourceInTheCluster :: boolean(),
                           NodeForWhichAlarmWasSetOrCleared :: node()}.

%%--------------------------------------------------------------------------

-spec start_link(pid(), any()) -> rabbit_types:ok(pid()).

start_link(HelperSup, Ref) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [self(), HelperSup, Ref]),

    {ok, Pid}.

-spec shutdown(pid(), string()) -> 'ok'.

shutdown(Pid, Explanation) ->
    gen_server:call(Pid, {shutdown, Explanation}, infinity).

-spec init(pid(), pid(), any()) -> no_return().

init(Parent, HelperSup, Ref) ->
    ?LG_PROCESS_TYPE(reader),
    {ok, Sock} = rabbit_networking:handshake(Ref,
        application:get_env(rabbit, proxy_protocol, false)),
    Deb = sys:debug_options([]),
    start_connection(Parent, HelperSup, Deb, Sock).

-spec system_continue(_,_,{[binary()], non_neg_integer(), #v1{}}) -> any().

system_continue(Parent, Deb, {Buf, BufLen, State}) ->
    mainloop(Deb, Buf, BufLen, State#v1{parent = Parent}).

-spec system_terminate(_,_,_,_) -> no_return().

system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

-spec system_code_change(_,_,_,_) -> {'ok',_}.

system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

-spec info_keys() -> rabbit_types:info_keys().

info_keys() -> ?INFO_KEYS.

-spec info(pid()) -> rabbit_types:infos().

info(Pid) ->
    gen_server:call(Pid, info, infinity).

-spec info(pid(), rabbit_types:info_keys()) -> rabbit_types:infos().

info(Pid, Items) ->
    case gen_server:call(Pid, {info, Items}, infinity) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

-spec force_event_refresh(pid(), reference()) -> 'ok'.

% Note: https://www.pivotaltracker.com/story/show/166962656
% This event is necessary for the stats timer to be initialized with
% the correct values once the management agent has started
force_event_refresh(Pid, Ref) ->
    gen_server:cast(Pid, {force_event_refresh, Ref}).

-spec conserve_resources(pid(), atom(), resource_alert()) -> 'ok'.

conserve_resources(Pid, Source, {_, Conserve, _}) ->
    Pid ! {conserve_resources, Source, Conserve},
    ok.

-spec server_properties(rabbit_types:protocol()) ->
          rabbit_framing:amqp_table().

server_properties(Protocol) ->
    {ok, Product} = application:get_key(rabbit, description),
    {ok, Version} = application:get_key(rabbit, vsn),

    %% Get any configuration-specified server properties
    {ok, RawConfigServerProps} = application:get_env(rabbit,
                                                     server_properties),

    %% Normalize the simplified (2-tuple) and unsimplified (3-tuple) forms
    %% from the config and merge them with the generated built-in properties
    NormalizedConfigServerProps =
        [{<<"capabilities">>, table, server_capabilities(Protocol)} |
         [case X of
              {KeyAtom, Value} -> {list_to_binary(atom_to_list(KeyAtom)),
                                   longstr,
                                   maybe_list_to_binary(Value)};
              {BinKey, Type, Value} -> {BinKey, Type, Value}
          end || X <- RawConfigServerProps ++
                     [{product,      Product},
                      {version,      Version},
                      {cluster_name, rabbit_nodes:cluster_name()},
                      {platform,     rabbit_misc:platform_and_version()},
                      {copyright,    ?COPYRIGHT_MESSAGE},
                      {information,  ?INFORMATION_MESSAGE}]]],

    %% Filter duplicated properties in favour of config file provided values
    lists:usort(fun ({K1,_,_}, {K2,_,_}) -> K1 =< K2 end,
                NormalizedConfigServerProps).

maybe_list_to_binary(V) when is_binary(V) -> V;
maybe_list_to_binary(V) when is_list(V)   -> list_to_binary(V).

server_capabilities(rabbit_framing_amqp_0_9_1) ->
    [{<<"publisher_confirms">>,           bool, true},
     {<<"exchange_exchange_bindings">>,   bool, true},
     {<<"basic.nack">>,                   bool, true},
     {<<"consumer_cancel_notify">>,       bool, true},
     {<<"connection.blocked">>,           bool, true},
     {<<"consumer_priorities">>,          bool, true},
     {<<"authentication_failure_close">>, bool, true},
     {<<"per_consumer_qos">>,             bool, true},
     {<<"direct_reply_to">>,              bool, true}];
server_capabilities(_) ->
    [].

%%--------------------------------------------------------------------------

socket_error(Reason) when is_atom(Reason) ->
    _ = rabbit_log_connection:error("Error on AMQP connection ~p: ~s~n",
        [self(), rabbit_misc:format_inet_error(Reason)]);
socket_error(Reason) ->
    Fmt = "Error on AMQP connection ~p:~n~p~n",
    Args = [self(), Reason],
    case Reason of
        %% The socket was closed while upgrading to SSL.
        %% This is presumably a TCP healthcheck, so don't log
        %% it unless specified otherwise.
        {ssl_upgrade_error, closed} ->
            %% Lager sinks (rabbit_log_connection)
            %% are handled by the lager parse_transform.
            %% Hence have to define the loglevel as a function call.
            _ = rabbit_log_connection:debug(Fmt, Args);
        _ ->
            _ = rabbit_log_connection:error(Fmt, Args)
    end.

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

socket_op(Sock, Fun) ->
    RealSocket = rabbit_net:unwrap_socket(Sock),
    case Fun(Sock) of
        {ok, Res}       -> Res;
        {error, Reason} -> socket_error(Reason),
                           rabbit_net:fast_close(RealSocket),
                           exit(normal)
    end.

-spec start_connection(pid(), pid(), any(), rabbit_net:socket()) ->
          no_return().

start_connection(Parent, HelperSup, Deb, Sock) ->
    process_flag(trap_exit, true),
    RealSocket = rabbit_net:unwrap_socket(Sock),
    Name = case rabbit_net:connection_string(Sock, inbound) of
               {ok, Str}         -> list_to_binary(Str);
               {error, enotconn} -> rabbit_net:fast_close(RealSocket),
                                    exit(normal);
               {error, Reason}   -> socket_error(Reason),
                                    rabbit_net:fast_close(RealSocket),
                                    exit(normal)
           end,
    {ok, HandshakeTimeout} = application:get_env(rabbit, handshake_timeout),
    InitialFrameMax = application:get_env(rabbit, initial_frame_max, ?FRAME_MIN_SIZE),
    erlang:send_after(HandshakeTimeout, self(), handshake_timeout),
    {PeerHost, PeerPort, Host, Port} =
        socket_op(Sock, fun (S) -> rabbit_net:socket_ends(S, inbound) end),
    ?store_proc_name(Name),
    State = #v1{parent              = Parent,
                sock                = RealSocket,
                connection          = #connection{
                  name               = Name,
                  log_name           = Name,
                  host               = Host,
                  peer_host          = PeerHost,
                  port               = Port,
                  peer_port          = PeerPort,
                  protocol           = none,
                  user               = none,
                  timeout_sec        = (HandshakeTimeout / 1000),
                  frame_max          = InitialFrameMax,
                  vhost              = none,
                  client_properties  = none,
                  capabilities       = [],
                  auth_mechanism     = none,
                  auth_state         = none,
                  connected_at       = os:system_time(
                                         milli_seconds)},
                callback            = uninitialized_callback,
                recv_len            = 0,
                pending_recv        = false,
                connection_state    = pre_init,
                queue_collector     = undefined,  %% started on tune-ok
                helper_sup          = HelperSup,
                heartbeater         = none,
                channel_sup_sup_pid = none,
                channel_count       = 0,
                throttle            = #throttle{
                                         last_blocked_at = never,
                                         should_block = false,
                                         blocked_by = sets:new(),
                                         connection_blocked_message_sent = false
                                         },
                proxy_socket = rabbit_net:maybe_get_proxy_socket(Sock)},
    try
        case run({?MODULE, recvloop,
                  [Deb, [], 0, switch_callback(rabbit_event:init_stats_timer(
                                                 State, #v1.stats_timer),
                                               handshake, 8)]}) of
            %% connection was closed cleanly by the client
            #v1{connection = #connection{user  = #user{username = Username},
                                         vhost = VHost}} ->
                _ = rabbit_log_connection:info("closing AMQP connection ~p (~s, vhost: '~s', user: '~s')~n",
                    [self(), dynamic_connection_name(Name), VHost, Username]);
            %% just to be more defensive
            _ ->
                _ = rabbit_log_connection:info("closing AMQP connection ~p (~s)~n",
                    [self(), dynamic_connection_name(Name)])
            end
    catch
        Ex ->
          log_connection_exception(dynamic_connection_name(Name), Ex)
    after
        %% We don't call gen_tcp:close/1 here since it waits for
        %% pending output to be sent, which results in unnecessary
        %% delays. We could just terminate - the reader is the
        %% controlling process and hence its termination will close
        %% the socket. However, to keep the file_handle_cache
        %% accounting as accurate as possible we ought to close the
        %% socket w/o delay before termination.
        rabbit_net:fast_close(RealSocket),
        rabbit_networking:unregister_connection(self()),
        rabbit_core_metrics:connection_closed(self()),
        ClientProperties = case get(client_properties) of
            undefined ->
                [];
            Properties ->
                Properties
        end,
        EventProperties = [{name, Name},
                           {pid, self()},
                           {node, node()},
                           {client_properties, ClientProperties}],
        EventProperties1 = case get(connection_user_provided_name) of
           undefined ->
               EventProperties;
           ConnectionUserProvidedName ->
               [{user_provided_name, ConnectionUserProvidedName} | EventProperties]
        end,
        rabbit_event:notify(connection_closed, EventProperties1)
    end,
    done.

log_connection_exception(Name, Ex) ->
    Severity = case Ex of
                   connection_closed_with_no_data_received -> debug;
                   {connection_closed_abruptly, _}         -> warning;
                   connection_closed_abruptly              -> warning;
                   _                                       -> error
               end,
    log_connection_exception(Severity, Name, Ex).

log_connection_exception(Severity, Name, {heartbeat_timeout, TimeoutSec}) ->
    %% Long line to avoid extra spaces and line breaks in log
    log_connection_exception_with_severity(Severity,
        "closing AMQP connection ~p (~s):~n"
        "missed heartbeats from client, timeout: ~ps~n",
        [self(), Name, TimeoutSec]);
log_connection_exception(Severity, Name, {connection_closed_abruptly,
                                          #v1{connection = #connection{user  = #user{username = Username},
                                                                       vhost = VHost}}}) ->
    log_connection_exception_with_severity(Severity,
        "closing AMQP connection ~p (~s, vhost: '~s', user: '~s'):~nclient unexpectedly closed TCP connection~n",
        [self(), Name, VHost, Username]);
%% when client abruptly closes connection before connection.open/authentication/authorization
%% succeeded, don't log username and vhost as 'none'
log_connection_exception(Severity, Name, {connection_closed_abruptly, _}) ->
    log_connection_exception_with_severity(Severity,
        "closing AMQP connection ~p (~s):~nclient unexpectedly closed TCP connection~n",
        [self(), Name]);
%% failed connection.tune negotiations
log_connection_exception(Severity, Name, {handshake_error, tuning, _Channel,
                                          {exit, #amqp_error{explanation = Explanation},
                                           _Method, _Stacktrace}}) ->
    log_connection_exception_with_severity(Severity,
        "closing AMQP connection ~p (~s):~nfailed to negotiate connection parameters: ~s~n",
        [self(), Name, Explanation]);
%% old exception structure
log_connection_exception(Severity, Name, connection_closed_abruptly) ->
    log_connection_exception_with_severity(Severity,
        "closing AMQP connection ~p (~s):~n"
        "client unexpectedly closed TCP connection~n",
        [self(), Name]);
log_connection_exception(Severity, Name, Ex) ->
    log_connection_exception_with_severity(Severity,
        "closing AMQP connection ~p (~s):~n~p~n",
        [self(), Name, Ex]).

log_connection_exception_with_severity(Severity, Fmt, Args) ->
    case Severity of
        debug   -> _ = rabbit_log_connection:debug(Fmt, Args);
        warning -> _ = rabbit_log_connection:warning(Fmt, Args);
        error   -> _ = rabbit_log_connection:error(Fmt, Args)
    end.

run({M, F, A}) ->
    try apply(M, F, A)
    catch {become, MFA} -> run(MFA)
    end.

recvloop(Deb, Buf, BufLen, State = #v1{pending_recv = true}) ->
    mainloop(Deb, Buf, BufLen, State);
recvloop(Deb, Buf, BufLen, State = #v1{connection_state = blocked}) ->
    mainloop(Deb, Buf, BufLen, State);
recvloop(Deb, Buf, BufLen, State = #v1{connection_state = {become, F}}) ->
    throw({become, F(Deb, Buf, BufLen, State)});
recvloop(Deb, Buf, BufLen, State = #v1{sock = Sock, recv_len = RecvLen})
  when BufLen < RecvLen ->
    case rabbit_net:setopts(Sock, [{active, once}]) of
        ok              -> mainloop(Deb, Buf, BufLen,
                                    State#v1{pending_recv = true});
        {error, Reason} -> stop(Reason, State)
    end;
recvloop(Deb, [B], _BufLen, State) ->
    {Rest, State1} = handle_input(State#v1.callback, B, State),
    recvloop(Deb, [Rest], size(Rest), State1);
recvloop(Deb, Buf, BufLen, State = #v1{recv_len = RecvLen}) ->
    {DataLRev, RestLRev} = binlist_split(BufLen - RecvLen, Buf, []),
    Data = list_to_binary(lists:reverse(DataLRev)),
    {<<>>, State1} = handle_input(State#v1.callback, Data, State),
    recvloop(Deb, lists:reverse(RestLRev), BufLen - RecvLen, State1).

binlist_split(0, L, Acc) ->
    {L, Acc};
binlist_split(Len, L, [Acc0|Acc]) when Len < 0 ->
    {H, T} = split_binary(Acc0, -Len),
    {[H|L], [T|Acc]};
binlist_split(Len, [H|T], Acc) ->
    binlist_split(Len - size(H), T, [H|Acc]).

-spec mainloop(_,[binary()], non_neg_integer(), #v1{}) -> any().

mainloop(Deb, Buf, BufLen, State = #v1{sock = Sock,
                                       connection_state = CS,
                                       connection = #connection{
                                         name = ConnName}}) ->
    Recv = rabbit_net:recv(Sock),
    case CS of
        pre_init when Buf =:= [] ->
            %% We only log incoming connections when either the
            %% first byte was received or there was an error (eg. a
            %% timeout).
            %%
            %% The goal is to not log TCP healthchecks (a connection
            %% with no data received) unless specified otherwise.
            Fmt = "accepting AMQP connection ~p (~s)~n",
            Args = [self(), ConnName],
            case Recv of
                closed -> _ = rabbit_log_connection:debug(Fmt, Args);
                _      -> _ = rabbit_log_connection:info(Fmt, Args)
            end;
        _ ->
            ok
    end,
    case Recv of
        {data, Data} ->
            recvloop(Deb, [Data | Buf], BufLen + size(Data),
                     State#v1{pending_recv = false});
        closed when State#v1.connection_state =:= closed ->
            State;
        closed when CS =:= pre_init andalso Buf =:= [] ->
            stop(tcp_healthcheck, State);
        closed ->
            stop(closed, State);
        {other, {heartbeat_send_error, Reason}} ->
            %% The only portable way to detect disconnect on blocked
            %% connection is to wait for heartbeat send failure.
            stop(Reason, State);
        {error, Reason} ->
            stop(Reason, State);
        {other, {system, From, Request}} ->
            sys:handle_system_msg(Request, From, State#v1.parent,
                                  ?MODULE, Deb, {Buf, BufLen, State});
        {other, Other}  ->
            case handle_other(Other, State) of
                stop     -> State;
                NewState -> recvloop(Deb, Buf, BufLen, NewState)
            end
    end.

-spec stop(_, #v1{}) -> no_return().
stop(tcp_healthcheck, State) ->
    %% The connection was closed before any packet was received. It's
    %% probably a load-balancer healthcheck: don't consider this a
    %% failure.
    maybe_emit_stats(State),
    throw(connection_closed_with_no_data_received);
stop(closed, State) ->
    maybe_emit_stats(State),
    throw({connection_closed_abruptly, State});
stop(Reason, State) ->
    maybe_emit_stats(State),
    throw({inet_error, Reason}).

handle_other({conserve_resources, Source, Conserve},
             State = #v1{throttle = Throttle = #throttle{blocked_by = Blockers}}) ->
  Resource  = {resource, Source},
  Blockers1 = case Conserve of
              true  -> sets:add_element(Resource, Blockers);
              false -> sets:del_element(Resource, Blockers)
          end,
    control_throttle(State#v1{throttle = Throttle#throttle{blocked_by = Blockers1}});
handle_other({channel_closing, ChPid}, State) ->
    ok = rabbit_channel:ready_for_close(ChPid),
    {_, State1} = channel_cleanup(ChPid, State),
    maybe_close(control_throttle(State1));
handle_other({'EXIT', Parent, normal}, State = #v1{parent = Parent}) ->
    %% rabbitmq/rabbitmq-server#544
    %% The connection port process has exited due to the TCP socket being closed.
    %% Handle this case in the same manner as receiving {error, closed}
    stop(closed, State);
handle_other({'EXIT', Parent, Reason}, State = #v1{parent = Parent}) ->
    Msg = io_lib:format("broker forced connection closure with reason '~w'", [Reason]),
    terminate(Msg, State),
    %% this is what we are expected to do according to
    %% https://www.erlang.org/doc/man/sys.html
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
handle_other(heartbeat_timeout,
             State = #v1{connection = #connection{timeout_sec = T}}) ->
    maybe_emit_stats(State),
    throw({heartbeat_timeout, T});
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
handle_other({'$gen_cast', {force_event_refresh, Ref}}, State)
  when ?IS_RUNNING(State) ->
    rabbit_event:notify(
      connection_created,
      augment_infos_with_user_provided_connection_name(
          [{type, network} | infos(?CREATION_EVENT_KEYS, State)], State),
      Ref),
    rabbit_event:init_stats_timer(State, #v1.stats_timer);
handle_other({'$gen_cast', {force_event_refresh, _Ref}}, State) ->
    %% Ignore, we will emit a created event once we start running.
    State;
handle_other(ensure_stats, State) ->
    ensure_stats_timer(State);
handle_other(emit_stats, State) ->
    emit_stats(State);
handle_other({bump_credit, Msg}, State) ->
    %% Here we are receiving credit by some channel process.
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
                                connection_forced, "~s", [Explanation], none))};
terminate(_Explanation, State) ->
    {force, State}.

send_blocked(#v1{connection = #connection{protocol     = Protocol,
                                          capabilities = Capabilities},
                 sock       = Sock}, Reason) ->
    case rabbit_misc:table_lookup(Capabilities, <<"connection.blocked">>) of
        {bool, true} ->

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

%%--------------------------------------------------------------------------
%% error handling / termination

close_connection(State = #v1{queue_collector = Collector,
                             connection = #connection{
                               timeout_sec = TimeoutSec}}) ->
    %% The spec says "Exclusive queues may only be accessed by the
    %% current connection, and are deleted when that connection
    %% closes."  This does not strictly imply synchrony, but in
    %% practice it seems to be what people assume.
    clean_up_exclusive_queues(Collector),
    %% We terminate the connection after the specified interval, but
    %% no later than ?CLOSING_TIMEOUT seconds.
    erlang:send_after((if TimeoutSec > 0 andalso
                          TimeoutSec < ?CLOSING_TIMEOUT -> TimeoutSec;
                          true                          -> ?CLOSING_TIMEOUT
                       end) * 1000, self(), terminate_connection),
    State#v1{connection_state = closed}.

%% queue collector will be undefined when connection
%% tuning was never performed or didn't finish. In such cases
%% there's also nothing to clean up.
clean_up_exclusive_queues(undefined) ->
    ok;

clean_up_exclusive_queues(Collector) ->
    rabbit_queue_collector:delete_all(Collector).

handle_dependent_exit(ChPid, Reason, State) ->
    {Channel, State1} = channel_cleanup(ChPid, State),
    case {Channel, termination_kind(Reason)} of
        {undefined,   controlled} -> State1;
        {undefined, uncontrolled} -> handle_uncontrolled_channel_close(ChPid),
                                     exit({abnormal_dependent_exit,
                                           ChPid, Reason});
        {_,           controlled} -> maybe_close(control_throttle(State1));
        {_,         uncontrolled} -> handle_uncontrolled_channel_close(ChPid),
                                     State2 = handle_exception(
                                                State1, Channel, Reason),
                                     maybe_close(control_throttle(State2))
    end.

terminate_channels(#v1{channel_count = 0} = State) ->
    State;
terminate_channels(#v1{channel_count = ChannelCount} = State) ->
    lists:foreach(fun rabbit_channel:shutdown/1, all_channels()),
    Timeout = 1000 * ?CHANNEL_TERMINATION_TIMEOUT * ChannelCount,
    TimerRef = erlang:send_after(Timeout, self(), cancel_wait),
    wait_for_channel_termination(ChannelCount, TimerRef, State).

wait_for_channel_termination(0, TimerRef, State) ->
    case erlang:cancel_timer(TimerRef) of
        false -> receive
                     cancel_wait -> State
                 end;
        _     -> State
    end;
wait_for_channel_termination(N, TimerRef,
                             State = #v1{connection_state = CS,
                                         connection = #connection{
                                                         log_name  = ConnName,
                                                         user      = User,
                                                         vhost     = VHost},
                                         sock = Sock}) ->
    receive
        {'DOWN', _MRef, process, ChPid, Reason} ->
            {Channel, State1} = channel_cleanup(ChPid, State),
            case {Channel, termination_kind(Reason)} of
                {undefined,    _} ->
                    exit({abnormal_dependent_exit, ChPid, Reason});
                {_,   controlled} ->
                    wait_for_channel_termination(N-1, TimerRef, State1);
                {_, uncontrolled} ->
                    _ = rabbit_log_connection:error(
                        "Error on AMQP connection ~p (~s, vhost: '~s',"
                        " user: '~s', state: ~p), channel ~p:"
                        "error while terminating:~n~p~n",
                        [self(), ConnName, VHost, User#user.username,
                         CS, Channel, Reason]),
                    handle_uncontrolled_channel_close(ChPid),
                    wait_for_channel_termination(N-1, TimerRef, State1)
            end;
        {'EXIT', Sock, _Reason} ->
            clean_up_all_channels(State),
            exit(normal);
        cancel_wait ->
            exit(channel_termination_timeout)
    end.

maybe_close(State = #v1{connection_state = closing,
                        channel_count    = 0,
                        connection       = #connection{protocol = Protocol},
                        sock             = Sock}) ->
    NewState = close_connection(State),
    ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
    NewState;
maybe_close(State) ->
    State.

termination_kind(normal) -> controlled;
termination_kind(_)      -> uncontrolled.

format_hard_error(#amqp_error{name = N, explanation = E, method = M}) ->
    io_lib:format("operation ~s caused a connection exception ~s: ~p", [M, N, E]);
format_hard_error(Reason) ->
    case io_lib:deep_char_list(Reason) of
        true  -> Reason;
        false -> rabbit_misc:format("~p", [Reason])
    end.

log_hard_error(#v1{connection_state = CS,
                   connection = #connection{
                                   log_name  = ConnName,
                                   user  = User,
                                   vhost = VHost}}, Channel, Reason) ->
    _ = rabbit_log_connection:error(
        "Error on AMQP connection ~p (~s, vhost: '~s',"
        " user: '~s', state: ~p), channel ~p:~n ~s~n",
        [self(), ConnName, VHost, User#user.username, CS, Channel, format_hard_error(Reason)]).

handle_exception(State = #v1{connection_state = closed}, Channel, Reason) ->
    log_hard_error(State, Channel, Reason),
    State;
handle_exception(State = #v1{connection = #connection{protocol = Protocol},
                             connection_state = CS},
                 Channel, Reason)
  when ?IS_RUNNING(State) orelse CS =:= closing ->
    respond_and_close(State, Channel, Protocol, Reason, Reason);
%% authentication failure
handle_exception(State = #v1{connection = #connection{protocol = Protocol,
                                                      log_name = ConnName,
                                                      capabilities = Capabilities},
                             connection_state = starting},
                 Channel, Reason = #amqp_error{name = access_refused,
                                               explanation = ErrMsg}) ->
    _ = rabbit_log_connection:error(
        "Error on AMQP connection ~p (~s, state: ~p):~n~s~n",
        [self(), ConnName, starting, ErrMsg]),
    %% respect authentication failure notification capability
    case rabbit_misc:table_lookup(Capabilities,
                                  <<"authentication_failure_close">>) of
        {bool, true} ->
            send_error_on_channel0_and_close(Channel, Protocol, Reason, State);
        _ ->
            close_connection(terminate_channels(State))
    end;
%% when loopback-only user tries to connect from a non-local host
%% when user tries to access a vhost it has no permissions for
handle_exception(State = #v1{connection = #connection{protocol = Protocol,
                                                      log_name = ConnName,
                                                      user = User},
                             connection_state = opening},
                 Channel, Reason = #amqp_error{name = not_allowed,
                                               explanation = ErrMsg}) ->
    _ = rabbit_log_connection:error(
        "Error on AMQP connection ~p (~s, user: '~s', state: ~p):~n~s~n",
        [self(), ConnName, User#user.username, opening, ErrMsg]),
    send_error_on_channel0_and_close(Channel, Protocol, Reason, State);
handle_exception(State = #v1{connection = #connection{protocol = Protocol},
                             connection_state = CS = opening},
                 Channel, Reason = #amqp_error{}) ->
    respond_and_close(State, Channel, Protocol, Reason,
                      {handshake_error, CS, Reason});
%% when negotiation fails, e.g. due to channel_max being higher than the
%% maximum allowed limit
handle_exception(State = #v1{connection = #connection{protocol = Protocol,
                                                      log_name = ConnName,
                                                      user = User},
                             connection_state = tuning},
                 Channel, Reason = #amqp_error{name = not_allowed,
                                               explanation = ErrMsg}) ->
    _ = rabbit_log_connection:error(
        "Error on AMQP connection ~p (~s,"
        " user: '~s', state: ~p):~n~s~n",
        [self(), ConnName, User#user.username, tuning, ErrMsg]),
    send_error_on_channel0_and_close(Channel, Protocol, Reason, State);
handle_exception(State, Channel, Reason) ->
    %% We don't trust the client at this point - force them to wait
    %% for a bit so they can't DOS us with repeated failed logins etc.
    timer:sleep(?SILENT_CLOSE_DELAY * 1000),
    throw({handshake_error, State#v1.connection_state, Channel, Reason}).

%% we've "lost sync" with the client and hence must not accept any
%% more input
-spec fatal_frame_error(_, _, _, _, _) -> no_return().
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

create_channel(_Channel,
               #v1{channel_count = ChannelCount,
                   connection    = #connection{channel_max = ChannelMax}})
  when ChannelMax /= 0 andalso ChannelCount >= ChannelMax ->
    {error, rabbit_misc:amqp_error(
              not_allowed, "number of channels opened (~w) has reached the "
              "negotiated channel_max (~w)",
              [ChannelCount, ChannelMax], 'none')};
create_channel(Channel,
               #v1{sock                = Sock,
                   queue_collector     = Collector,
                   channel_sup_sup_pid = ChanSupSup,
                   channel_count       = ChannelCount,
                   connection =
                       #connection{name         = Name,
                                   protocol     = Protocol,
                                   frame_max    = FrameMax,
                                   vhost        = VHost,
                                   capabilities = Capabilities,
                                   user = #user{username = Username} = User}
                   } = State) ->
    case rabbit_auth_backend_internal:is_over_channel_limit(Username) of
        false ->
            {ok, _ChSupPid, {ChPid, AState}} =
                rabbit_channel_sup_sup:start_channel(
                  ChanSupSup, {tcp, Sock, Channel, FrameMax, self(), Name,
                               Protocol, User, VHost, Capabilities,
                               Collector}),
            MRef = erlang:monitor(process, ChPid),
            put({ch_pid, ChPid}, {Channel, MRef}),
            put({channel, Channel}, {ChPid, AState}),
            {ok, {ChPid, AState}, State#v1{channel_count = ChannelCount + 1}};
        {true, Limit} ->
            {error, rabbit_misc:amqp_error(not_allowed,
                        "number of channels opened for user '~s' has reached "
                        "the maximum allowed user limit of (~w)",
                        [Username, Limit], 'none')}
    end.

channel_cleanup(ChPid, State = #v1{channel_count = ChannelCount}) ->
    case get({ch_pid, ChPid}) of
        undefined       -> {undefined, State};
        {Channel, MRef} -> credit_flow:peer_down(ChPid),
                           erase({channel, Channel}),
                           erase({ch_pid, ChPid}),
                           erlang:demonitor(MRef, [flush]),
                           {Channel, State#v1{channel_count = ChannelCount - 1}}
    end.

all_channels() -> [ChPid || {{ch_pid, ChPid}, _ChannelMRef} <- get()].

clean_up_all_channels(State) ->
    CleanupFun = fun(ChPid) ->
                    channel_cleanup(ChPid, State)
                 end,
    lists:foreach(CleanupFun, all_channels()).

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
    case (case get(ChKey) of
              undefined -> create_channel(Channel, State);
              Other     -> {ok, Other, State}
          end) of
        {error, Error} ->
            handle_exception(State, Channel, Error);
        {ok, {ChPid, AState}, State1} ->
            case rabbit_command_assembler:process(Frame, AState) of
                {ok, NewAState} ->
                    put(ChKey, {ChPid, NewAState}),
                    post_process_frame(Frame, ChPid, State1);
                {ok, Method, NewAState} ->
                    rabbit_channel:do(ChPid, Method),
                    put(ChKey, {ChPid, NewAState}),
                    post_process_frame(Frame, ChPid, State1);
                {ok, Method, Content, NewAState} ->
                    rabbit_channel:do_flow(ChPid, Method, Content),
                    put(ChKey, {ChPid, NewAState}),
                    post_process_frame(Frame, ChPid, control_throttle(State1));
                {error, Reason} ->
                    handle_exception(State1, Channel, Reason)
            end
    end.

post_process_frame({method, 'channel.close_ok', _}, ChPid, State) ->
    {_, State1} = channel_cleanup(ChPid, State),
    %% This is not strictly necessary, but more obviously
    %% correct. Also note that we do not need to call maybe_close/1
    %% since we cannot possibly be in the 'closing' state.
    control_throttle(State1);
post_process_frame({content_header, _, _, _, _}, _ChPid, State) ->
    publish_received(State);
post_process_frame({content_body, _}, _ChPid, State) ->
    publish_received(State);
post_process_frame(_Frame, _ChPid, State) ->
    State.

%%--------------------------------------------------------------------------

%% We allow clients to exceed the frame size a little bit since quite
%% a few get it wrong - off-by 1 or 8 (empty frame size) are typical.
-define(FRAME_SIZE_FUDGE, ?EMPTY_FRAME_SIZE).

handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32, _/binary>>,
             State = #v1{connection = #connection{frame_max = FrameMax}})
  when FrameMax /= 0 andalso
       PayloadSize > FrameMax - ?EMPTY_FRAME_SIZE + ?FRAME_SIZE_FUDGE ->
    fatal_frame_error(
      {frame_too_large, PayloadSize, FrameMax - ?EMPTY_FRAME_SIZE},
      Type, Channel, <<>>, State);
handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32,
                             Payload:PayloadSize/binary, ?FRAME_END,
                             Rest/binary>>,
             State) ->
    {Rest, ensure_stats_timer(handle_frame(Type, Channel, Payload, State))};
handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32, Rest/binary>>,
             State) ->
    {Rest, ensure_stats_timer(
             switch_callback(State,
                             {frame_payload, Type, Channel, PayloadSize},
                             PayloadSize + 1))};
handle_input({frame_payload, Type, Channel, PayloadSize}, Data, State) ->
    <<Payload:PayloadSize/binary, EndMarker, Rest/binary>> = Data,
    case EndMarker of
        ?FRAME_END -> State1 = handle_frame(Type, Channel, Payload, State),
                      {Rest, switch_callback(State1, frame_header, 7)};
        _          -> fatal_frame_error({invalid_frame_end_marker, EndMarker},
                                        Type, Channel, Payload, State)
    end;
handle_input(handshake, <<"AMQP", A, B, C, D, Rest/binary>>, State) ->
    {Rest, handshake({A, B, C, D}, State)};
handle_input(handshake, <<Other:8/binary, _/binary>>, #v1{sock = Sock}) ->
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

-spec refuse_connection(_, _, _) -> no_return().
refuse_connection(Sock, Exception, {A, B, C, D}) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, <<"AMQP",A,B,C,D>>) end),
    throw(Exception).

-spec refuse_connection(rabbit_net:socket(), any()) -> no_return().

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
    catch throw:{inet_error, E} when E =:= closed; E =:= enotconn ->
            maybe_emit_stats(State),
            throw({connection_closed_abruptly, State});
          exit:#amqp_error{method = none} = Reason ->
            handle_exception(State, 0, Reason#amqp_error{method = MethodName});
          Type:Reason:Stacktrace ->
            handle_exception(State, 0, {Type, Reason, MethodName, Stacktrace})
    end.

handle_method0(#'connection.start_ok'{mechanism = Mechanism,
                                      response = Response,
                                      client_properties = ClientProperties},
               State0 = #v1{connection_state = starting,
                            connection       = Connection0,
                            sock             = Sock}) ->
    AuthMechanism = auth_mechanism_to_module(Mechanism, Sock),
    Capabilities =
        case rabbit_misc:table_lookup(ClientProperties, <<"capabilities">>) of
            {table, Capabilities1} -> Capabilities1;
            _                      -> []
        end,
    Connection1 = Connection0#connection{
                    client_properties = ClientProperties,
                    capabilities      = Capabilities,
                    auth_mechanism    = {Mechanism, AuthMechanism},
                    auth_state        = AuthMechanism:init(Sock)},
    Connection2 = augment_connection_log_name(Connection1),
    State = State0#v1{connection_state = securing,
                      connection       = Connection2},
    % adding client properties to process dictionary to send them later
    % in the connection_closed event
    put(client_properties, ClientProperties),
    case user_provided_connection_name(Connection2) of
        undefined ->
            undefined;
        UserProvidedConnectionName ->
            put(connection_user_provided_name, UserProvidedConnectionName)
    end,
    auth_phase(Response, State);

handle_method0(#'connection.secure_ok'{response = Response},
               State = #v1{connection_state = securing}) ->
    auth_phase(Response, State);

handle_method0(#'connection.tune_ok'{frame_max   = FrameMax,
                                     channel_max = ChannelMax,
                                     heartbeat   = ClientHeartbeat},
               State = #v1{connection_state = tuning,
                           connection = Connection,
                           helper_sup = SupPid,
                           sock = Sock}) ->
    ok = validate_negotiated_integer_value(
           frame_max,   ?FRAME_MIN_SIZE, FrameMax),
    ok = validate_negotiated_integer_value(
           channel_max, ?CHANNEL_MIN,    ChannelMax),
    {ok, Collector} = rabbit_connection_helper_sup:start_queue_collector(
                        SupPid, Connection#connection.name),
    Frame = rabbit_binary_generator:build_heartbeat_frame(),
    Parent = self(),
    SendFun =
        fun() ->
                case catch rabbit_net:send(Sock, Frame) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        Parent ! {heartbeat_send_error, Reason};
                    Unexpected ->
                        Parent ! {heartbeat_send_error, Unexpected}
                end,
                ok
        end,
    ReceiveFun = fun() -> Parent ! heartbeat_timeout end,
    Heartbeater = rabbit_heartbeat:start(
                    SupPid, Sock, Connection#connection.name,
                    ClientHeartbeat, SendFun, ClientHeartbeat, ReceiveFun),
    State#v1{connection_state = opening,
             connection = Connection#connection{
                            frame_max   = FrameMax,
                            channel_max = ChannelMax,
                            timeout_sec = ClientHeartbeat},
             queue_collector = Collector,
             heartbeater = Heartbeater};

handle_method0(#'connection.open'{virtual_host = VHost},
               State = #v1{connection_state = opening,
                           connection       = Connection = #connection{
                                                log_name = ConnName,
                                                user = User = #user{username = Username},
                                                protocol = Protocol},
                           helper_sup       = SupPid,
                           sock             = Sock,
                           throttle         = Throttle}) ->

    ok = is_over_vhost_connection_limit(VHost, User),
    ok = is_over_user_connection_limit(User),
    ok = rabbit_access_control:check_vhost_access(User, VHost, {socket, Sock}, #{}),
    ok = is_vhost_alive(VHost, User),
    NewConnection = Connection#connection{vhost = VHost},
    ok = send_on_channel0(Sock, #'connection.open_ok'{}, Protocol),

    Alarms = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    BlockedBy = sets:from_list([{resource, Alarm} || Alarm <- Alarms]),
    Throttle1 = Throttle#throttle{blocked_by = BlockedBy},

    {ok, ChannelSupSupPid} =
        rabbit_connection_helper_sup:start_channel_sup_sup(SupPid),
    State1 = control_throttle(
               State#v1{connection_state    = running,
                        connection          = NewConnection,
                        channel_sup_sup_pid = ChannelSupSupPid,
                        throttle            = Throttle1}),
    Infos = augment_infos_with_user_provided_connection_name(
        [{type, network} | infos(?CREATION_EVENT_KEYS, State1)],
        State1
    ),
    rabbit_core_metrics:connection_created(proplists:get_value(pid, Infos),
                                           Infos),
    rabbit_event:notify(connection_created, Infos),
    maybe_emit_stats(State1),
    _ = rabbit_log_connection:info(
        "connection ~p (~s): "
        "user '~s' authenticated and granted access to vhost '~s'~n",
        [self(), dynamic_connection_name(ConnName), Username, VHost]),
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
handle_method0(#'connection.update_secret'{new_secret = NewSecret, reason = Reason},
               State = #v1{connection =
                               #connection{protocol   = Protocol,
                                           user       = User = #user{username = Username},
                                           log_name   = ConnName} = Conn,
                           sock       = Sock}) when ?IS_RUNNING(State) ->
    _ = rabbit_log_connection:debug(
        "connection ~p (~s) of user '~s': "
        "asked to update secret, reason: ~s~n",
        [self(), dynamic_connection_name(ConnName), Username, Reason]),
    case rabbit_access_control:update_state(User, NewSecret) of
      {ok, User1} ->
        %% User/auth backend state has been updated. Now we can propagate it to channels
        %% asynchronously and return. All the channels have to do is to update their
        %% own state.
        %%
        %% Any secret update errors coming from the authz backend will be handled in the other branch.
        %% Therefore we optimistically do no error handling here. MK.
        lists:foreach(fun(Ch) ->
          _ = rabbit_log:debug("Updating user/auth backend state for channel ~p", [Ch]),
          _ = rabbit_channel:update_user_state(Ch, User1)
        end, all_channels()),
        ok = send_on_channel0(Sock, #'connection.update_secret_ok'{}, Protocol),
        _ = rabbit_log_connection:info(
            "connection ~p (~s): "
            "user '~s' updated secret, reason: ~s~n",
            [self(), dynamic_connection_name(ConnName), Username, Reason]),
        State#v1{connection = Conn#connection{user = User1}};
      {refused, Message} ->
        _ = rabbit_log_connection:error("Secret update was refused for user '~p': ~p",
                                    [Username, Message]),
        rabbit_misc:protocol_error(not_allowed, "New secret was refused by one of the backends", []);
      {error, Message} ->
        _ = rabbit_log_connection:error("Secret update for user '~p' failed: ~p",
                                    [Username, Message]),
        rabbit_misc:protocol_error(not_allowed,
                                  "Secret update failed", [])
    end;
handle_method0(_Method, State) when ?IS_STOPPING(State) ->
    State;
handle_method0(_Method, #v1{connection_state = S}) ->
    rabbit_misc:protocol_error(
      channel_error, "unexpected method in connection state ~w", [S]).

is_vhost_alive(VHostPath, User) ->
    case rabbit_vhost_sup_sup:is_vhost_alive(VHostPath) of
        true  -> ok;
        false ->
            rabbit_misc:protocol_error(internal_error,
                            "access to vhost '~s' refused for user '~s': "
                            "vhost '~s' is down",
                            [VHostPath, User#user.username, VHostPath])
    end.

is_over_vhost_connection_limit(VHostPath, User) ->
    try rabbit_vhost_limit:is_over_connection_limit(VHostPath) of
        false         -> ok;
        {true, Limit} -> rabbit_misc:protocol_error(not_allowed,
                            "access to vhost '~s' refused for user '~s': "
                            "connection limit (~p) is reached",
                            [VHostPath, User#user.username, Limit])
    catch
        throw:{error, {no_such_vhost, VHostPath}} ->
            rabbit_misc:protocol_error(not_allowed, "vhost ~s not found", [VHostPath])
    end.

is_over_user_connection_limit(#user{username = Username}) ->
    case rabbit_auth_backend_internal:is_over_connection_limit(Username) of
        false -> ok;
        {true, Limit} -> rabbit_misc:protocol_error(not_allowed,
                            "Connection refused for user '~s': "
                            "user connection limit (~p) is reached",
                            [Username, Limit])
    end.

validate_negotiated_integer_value(Field, Min, ClientValue) ->
    ServerValue = get_env(Field),
    if ClientValue /= 0 andalso ClientValue < Min ->
            fail_negotiation(Field, min, Min, ClientValue);
       ServerValue /= 0 andalso (ClientValue =:= 0 orelse
                                 ClientValue > ServerValue) ->
            fail_negotiation(Field, max, ServerValue, ClientValue);
       true ->
            ok
    end.

%% keep dialyzer happy
-spec fail_negotiation(atom(), 'min' | 'max', integer(), integer()) ->
          no_return().
fail_negotiation(Field, MinOrMax, ServerValue, ClientValue) ->
    {S1, S2} = case MinOrMax of
                   min -> {lower,  minimum};
                   max -> {higher, maximum}
               end,
    ClientValueDetail = get_client_value_detail(Field, ClientValue),
    rabbit_misc:protocol_error(
      not_allowed, "negotiated ~w = ~w~s is ~w than the ~w allowed value (~w)",
      [Field, ClientValue, ClientValueDetail, S1, S2, ServerValue], 'connection.tune').

get_env(Key) ->
    {ok, Value} = application:get_env(rabbit, Key),
    Value.

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
                                       auth_mechanism = {Name, AuthMechanism},
                                       auth_state     = AuthState},
                       sock = Sock}) ->
    _ = rabbit_log:debug("Raw client connection hostname during authN phase: ~p", [Connection#connection.host]),
    RemoteAddress = case Connection#connection.host of
        %% the hostname was already resolved, e.g. by reverse DNS lookups
        Bin when is_binary(Bin) -> Bin;
        %% the hostname is an IP address
        Tuple when is_tuple(Tuple) ->
            rabbit_data_coercion:to_binary(inet:ntoa(Connection#connection.host));
        Other -> rabbit_data_coercion:to_binary(Other)
    end,
    _ = rabbit_log:debug("Resolved client hostname during authN phase: ~s", [RemoteAddress]),
    case AuthMechanism:handle_response(Response, AuthState) of
        {refused, Username, Msg, Args} ->
            rabbit_core_metrics:auth_attempt_failed(RemoteAddress, Username, amqp091),
            auth_fail(Username, Msg, Args, Name, State);
        {protocol_error, Msg, Args} ->
            rabbit_core_metrics:auth_attempt_failed(RemoteAddress, <<>>, amqp091),
            notify_auth_result(none, user_authentication_failure,
                               [{error, rabbit_misc:format(Msg, Args)}],
                               State),
            rabbit_misc:protocol_error(syntax_error, Msg, Args);
        {challenge, Challenge, AuthState1} ->
            rabbit_core_metrics:auth_attempt_succeeded(RemoteAddress, <<>>, amqp091),
            Secure = #'connection.secure'{challenge = Challenge},
            ok = send_on_channel0(Sock, Secure, Protocol),
            State#v1{connection = Connection#connection{
                                    auth_state = AuthState1}};
        {ok, User = #user{username = Username}} ->
            case rabbit_access_control:check_user_loopback(Username, Sock) of
                ok ->
                    rabbit_core_metrics:auth_attempt_succeeded(RemoteAddress, Username, amqp091),
                    notify_auth_result(Username, user_authentication_success,
                                       [], State);
                not_allowed ->
                    rabbit_core_metrics:auth_attempt_failed(RemoteAddress, Username, amqp091),
                    auth_fail(Username, "user '~s' can only connect via "
                              "localhost", [Username], Name, State)
            end,
            Tune = #'connection.tune'{frame_max   = get_env(frame_max),
                                      channel_max = get_env(channel_max),
                                      heartbeat   = get_env(heartbeat)},
            ok = send_on_channel0(Sock, Tune, Protocol),
            State#v1{connection_state = tuning,
                     connection = Connection#connection{user       = User,
                                                        auth_state = none}}
    end.

-spec auth_fail
        (rabbit_types:username() | none, string(), [any()], binary(), #v1{}) ->
            no_return().

auth_fail(Username, Msg, Args, AuthName,
          State = #v1{connection = #connection{protocol     = Protocol,
                                               capabilities = Capabilities}}) ->
    notify_auth_result(Username, user_authentication_failure,
      [{error, rabbit_misc:format(Msg, Args)}], State),
    AmqpError = rabbit_misc:amqp_error(
                  access_refused, "~s login refused: ~s",
                  [AuthName, io_lib:format(Msg, Args)], none),
    case rabbit_misc:table_lookup(Capabilities,
                                  <<"authentication_failure_close">>) of
        {bool, true} ->
            SafeMsg = io_lib:format(
                        "Login was refused using authentication "
                        "mechanism ~s. For details see the broker "
                        "logfile.", [AuthName]),
            AmqpError1 = AmqpError#amqp_error{explanation = SafeMsg},
            {0, CloseMethod} = rabbit_binary_generator:map_exception(
                                 0, AmqpError1, Protocol),
            ok = send_on_channel0(State#v1.sock, CloseMethod, Protocol);
        _ -> ok
    end,
    rabbit_misc:protocol_error(AmqpError).

notify_auth_result(Username, AuthResult, ExtraProps, State) ->
    EventProps = [{connection_type, network},
                  {name, case Username of none -> ''; _ -> Username end}] ++
                 [case Item of
                      name -> {connection_name, i(name, State)};
                      _    -> {Item, i(Item, State)}
                  end || Item <- ?AUTH_NOTIFICATION_INFO_KEYS] ++
                 ExtraProps,
    rabbit_event:notify(AuthResult, [P || {_, V} = P <- EventProps, V =/= '']).

%%--------------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(pid,                #v1{}) -> self();
i(node,               #v1{}) -> node();
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
i(channels,           #v1{channel_count = ChannelCount}) -> ChannelCount;
i(state, #v1{connection_state = ConnectionState,
             throttle         = #throttle{blocked_by = Reasons,
                                          last_blocked_at = T} = Throttle}) ->
    %% not throttled by resource or other longer-term reasons
    %% TODO: come up with a sensible function name
    case sets:size(sets:del_element(flow, Reasons)) =:= 0 andalso
        (credit_flow:blocked()        %% throttled by flow now
         orelse                       %% throttled by flow recently
           (is_blocked_by_flow(Throttle) andalso T =/= never andalso
            erlang:convert_time_unit(erlang:monotonic_time() - T,
                                          native,
                                          micro_seconds) < 5000000)) of
        true  -> flow;
        false ->
          case {has_reasons_to_block(Throttle), ConnectionState} of
            %% blocked
            {_,    blocked} -> blocked;
            %% not yet blocked (there were no publishes)
            {true, running} -> blocking;
            %% not blocked
            {false,      _} -> ConnectionState;
            %% catch all to be defensive
            _               -> ConnectionState
          end
    end;
i(garbage_collection, _State) ->
    rabbit_misc:get_gc_info(self());
i(reductions, _State) ->
    {reductions, Reductions} = erlang:process_info(self(), reductions),
    Reductions;
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
ic(user_who_performed_action, C) -> ic(user, C);
ic(vhost,             #connection{vhost       = VHost})    -> VHost;
ic(timeout,           #connection{timeout_sec = Timeout})  -> Timeout;
ic(frame_max,         #connection{frame_max   = FrameMax}) -> FrameMax;
ic(channel_max,       #connection{channel_max = ChMax})    -> ChMax;
ic(client_properties, #connection{client_properties = CP}) -> CP;
ic(auth_mechanism,    #connection{auth_mechanism = none})  -> none;
ic(auth_mechanism,    #connection{auth_mechanism = {Name, _Mod}}) -> Name;
ic(connected_at,      #connection{connected_at = T}) -> T;
ic(Item,              #connection{}) -> throw({bad_argument, Item}).

socket_info(Get, Select, #v1{sock = Sock}) ->
    case Get(Sock) of
        {ok,    T} -> case Select(T) of
                          N when is_number(N) -> N;
                          _ -> 0
                      end;
        {error, _} -> 0
    end.

ssl_info(F, #v1{sock = Sock}) ->
    case rabbit_net:ssl_info(Sock) of
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

maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #v1.stats_timer,
                            fun() -> emit_stats(State) end).

emit_stats(State) ->
    [{_, Pid}, {_, Recv_oct}, {_, Send_oct}, {_, Reductions}] = I
      = infos(?SIMPLE_METRICS, State),
    Infos = infos(?OTHER_METRICS, State),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid, Recv_oct, Send_oct, Reductions),
    rabbit_event:notify(connection_stats, Infos ++ I),
    State1 = rabbit_event:reset_stats_timer(State, #v1.stats_timer),
    ensure_stats_timer(State1).

%% 1.0 stub
-spec become_1_0(non_neg_integer(), #v1{}) -> no_return().

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
                 F = fun (_Deb, Buf, BufLen, S) ->
                             {rabbit_amqp1_0_reader, init,
                              [Mode, pack_for_1_0(Buf, BufLen, S)]}
                     end,
                 State#v1{connection_state = {become, F}}
    end.

pack_for_1_0(Buf, BufLen, #v1{parent       = Parent,
                              sock         = Sock,
                              recv_len     = RecvLen,
                              pending_recv = PendingRecv,
                              helper_sup   = SupPid,
                              proxy_socket = ProxySocket}) ->
    {Parent, Sock, RecvLen, PendingRecv, SupPid, Buf, BufLen, ProxySocket}.

respond_and_close(State, Channel, Protocol, Reason, LogErr) ->
    log_hard_error(State, Channel, LogErr),
    send_error_on_channel0_and_close(Channel, Protocol, Reason, State).

send_error_on_channel0_and_close(Channel, Protocol, Reason, State) ->
    {0, CloseMethod} =
        rabbit_binary_generator:map_exception(Channel, Reason, Protocol),
    State1 = close_connection(terminate_channels(State)),
    ok = send_on_channel0(State#v1.sock, CloseMethod, Protocol),
    State1.

%%
%% Publisher throttling
%%

blocked_by_message(#throttle{blocked_by = Reasons}) ->
  %% we don't want to report internal flow as a reason here since
  %% it is entirely transient
  Reasons1 = sets:del_element(flow, Reasons),
  RStr = string:join([format_blocked_by(R) || R <- sets:to_list(Reasons1)], " & "),
  list_to_binary(rabbit_misc:format("low on ~s", [RStr])).

format_blocked_by({resource, memory}) -> "memory";
format_blocked_by({resource, disk})   -> "disk";
format_blocked_by({resource, disc})   -> "disk".

update_last_blocked_at(Throttle) ->
    Throttle#throttle{last_blocked_at = erlang:monotonic_time()}.

connection_blocked_message_sent(
    #throttle{connection_blocked_message_sent = BS}) -> BS.

should_send_blocked(Throttle = #throttle{blocked_by = Reasons}) ->
    should_block(Throttle)
    andalso
    sets:size(sets:del_element(flow, Reasons)) =/= 0
    andalso
    not connection_blocked_message_sent(Throttle).

should_send_unblocked(Throttle = #throttle{blocked_by = Reasons}) ->
    connection_blocked_message_sent(Throttle)
    andalso
    sets:size(sets:del_element(flow, Reasons)) == 0.

%% Returns true if we have a reason to block
%% this connection.
has_reasons_to_block(#throttle{blocked_by = Reasons}) ->
    sets:size(Reasons) > 0.

is_blocked_by_flow(#throttle{blocked_by = Reasons}) ->
    sets:is_element(flow, Reasons).

should_block(#throttle{should_block = Val}) -> Val.

should_block_connection(Throttle) ->
    should_block(Throttle) andalso has_reasons_to_block(Throttle).

should_unblock_connection(Throttle) ->
    not should_block_connection(Throttle).

maybe_block(State = #v1{connection_state = CS, throttle = Throttle}) ->
    case should_block_connection(Throttle) of
        true ->
            State1 = State#v1{connection_state = blocked,
                              throttle = update_last_blocked_at(Throttle)},
            case CS of
                running ->
                    ok = rabbit_heartbeat:pause_monitor(State#v1.heartbeater);
                _       -> ok
            end,
            maybe_send_blocked_or_unblocked(State1);
        false -> State
    end.

maybe_unblock(State = #v1{throttle = Throttle}) ->
    case should_unblock_connection(Throttle) of
        true ->
            ok = rabbit_heartbeat:resume_monitor(State#v1.heartbeater),
            State1 = State#v1{connection_state = running,
                              throttle = Throttle#throttle{should_block = false}},
            maybe_send_unblocked(State1);
        false -> State
    end.

maybe_send_unblocked(State = #v1{throttle = Throttle}) ->
    case should_send_unblocked(Throttle) of
        true ->
            ok = send_unblocked(State),
            State#v1{throttle =
                Throttle#throttle{connection_blocked_message_sent = false}};
        false -> State
    end.

maybe_send_blocked_or_unblocked(State = #v1{throttle = Throttle}) ->
    case should_send_blocked(Throttle) of
        true ->
            ok = send_blocked(State, blocked_by_message(Throttle)),
            State#v1{throttle =
                Throttle#throttle{connection_blocked_message_sent = true}};
        false -> maybe_send_unblocked(State)
    end.

publish_received(State = #v1{throttle = Throttle}) ->
    case has_reasons_to_block(Throttle) of
      false -> State;
      true  ->
        Throttle1 = Throttle#throttle{should_block = true},
        maybe_block(State#v1{throttle = Throttle1})
    end.

control_throttle(State = #v1{connection_state = CS,
                             throttle = #throttle{blocked_by = Reasons} = Throttle}) ->
    Throttle1 = case credit_flow:blocked() of
                  true  ->
                    Throttle#throttle{blocked_by = sets:add_element(flow, Reasons)};
                  false ->
                    Throttle#throttle{blocked_by = sets:del_element(flow, Reasons)}
             end,
    State1 = State#v1{throttle = Throttle1},
    case CS of
        running -> maybe_block(State1);
        %% unblock or re-enable blocking
        blocked -> maybe_block(maybe_unblock(State1));
        _       -> State1
    end.

augment_connection_log_name(#connection{name = Name} = Connection) ->
    case user_provided_connection_name(Connection) of
        undefined ->
            Connection;
        UserSpecifiedName ->
            LogName = <<Name/binary, " - ", UserSpecifiedName/binary>>,
            _ = rabbit_log_connection:info("Connection ~p (~s) has a client-provided name: ~s~n", [self(), Name, UserSpecifiedName]),
            ?store_proc_name(LogName),
            Connection#connection{log_name = LogName}
    end.

augment_infos_with_user_provided_connection_name(Infos, #v1{connection = Connection}) ->
    case user_provided_connection_name(Connection) of
     undefined ->
         Infos;
     UserProvidedConnectionName ->
         [{user_provided_name, UserProvidedConnectionName} | Infos]
    end.

user_provided_connection_name(#connection{client_properties = ClientProperties}) ->
    case rabbit_misc:table_lookup(ClientProperties, <<"connection_name">>) of
        {longstr, UserSpecifiedName} ->
            UserSpecifiedName;
        _ ->
            undefined
    end.

dynamic_connection_name(Default) ->
    case rabbit_misc:get_proc_name() of
        {ok, Name} ->
            Name;
        _ ->
            Default
    end.

handle_uncontrolled_channel_close(ChPid) ->
    rabbit_core_metrics:channel_closed(ChPid),
    rabbit_event:notify(channel_closed, [{pid, ChPid}]).

-spec get_client_value_detail(atom(), integer()) -> string().
get_client_value_detail(channel_max, 0) ->
    " (no limit)";
get_client_value_detail(_Field, _ClientValue) ->
    "".
