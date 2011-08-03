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

-module(rabbit_reader).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start_link/3, info_keys/0, info/1, info/2, shutdown/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/4, mainloop/2]).

-export([conserve_memory/2, ensure_stats_timer/1, server_frame_max/0]).

-export([all_channels/0, channel_cleanup/1, emit_stats/1, infos/2,
         internal_conserve_memory/2, maybe_close/1, send_exception/3,
         send_on_channel0/3, switch_callback/3]).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 1).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).
-define(SILENT_CLOSE_DELAY, 3).

-define(IS_RUNNING(State),
        (State#v1.connection_state =:= running orelse
         State#v1.connection_state =:= blocking orelse
         State#v1.connection_state =:= blocked)).

%%--------------------------------------------------------------------------

-record(v1, {parent, sock, connection, callback, recv_len, pending_recv,
             connection_state, queue_collector, heartbeater, stats_timer,
             channel_sup_sup_pid, start_heartbeat_fun, buf, buf_len,
             auth_mechanism, auth_state, module}).

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

%%--------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/3 :: (pid(), pid(), rabbit_heartbeat:start_heartbeat_fun()) ->
                           rabbit_types:ok(pid())).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (pid()) -> rabbit_types:infos()).
-spec(info/2 :: (pid(), rabbit_types:info_keys()) -> rabbit_types:infos()).
-spec(shutdown/2 :: (pid(), string()) -> 'ok').
-spec(conserve_memory/2 :: (pid(), boolean()) -> 'ok').

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
            ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
            NewState;
        _  -> State
    end;
maybe_close(State) ->
    State.

termination_kind(normal) -> controlled;
termination_kind(_)      -> uncontrolled.

handle_input(handshake, H = <<"AMQP", A, B, C, D>>, State = #v1{sock = Sock}) ->
    Modules = [M || {_, M} <- rabbit_registry:lookup_all(amqp)],
    case lists:foldl(
      fun (Module, not_accepted) ->
              case Module:accept_handshake_bytes(H) of
                  true  -> Module;
                  false -> not_accepted
              end;
          (_, Module) -> Module
      end, not_accepted, Modules) of
        not_accepted -> refuse_connection(Sock, {bad_version, A, B, C, D});
        Module       -> Module:start_connection(H, State#v1{module = Module})
    end;

handle_input(handshake, Other, #v1{sock = Sock}) ->
    refuse_connection(Sock, {bad_header, Other});

handle_input(Callback, Data, State = #v1{module = Module}) ->
    Module:handle_input(Callback, Data, State).

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

%% Compute frame_max for this instance. Could simply use 0, but breaks
%% QPid Java client.
server_frame_max() ->
    {ok, FrameMax} = application:get_env(rabbit, frame_max),
    FrameMax.

send_on_channel0(Sock, Method, Protocol) ->
    ok = rabbit_writer:internal_send_command(Sock, 0, Method, Protocol).

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
