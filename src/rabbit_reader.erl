%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_reader).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start_link/0, info/1, info/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/1, mainloop/3]).

-export([analyze_frame/2]).

-import(gen_tcp).
-import(fprof).
-import(inet).
-import(prim_inet).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 1).
-define(CHANNEL_CLOSING_TIMEOUT, 1).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).

%---------------------------------------------------------------------------

-record(v1, {sock, connection, callback, recv_ref, connection_state}).

-define(INFO_KEYS,
        [pid, address, port, peer_address, peer_port,
         recv_oct, recv_cnt, send_oct, send_cnt, send_pend,
         state, channels, user, vhost, timeout, frame_max]).

%% connection lifecycle
%%
%% all state transitions and terminations are marked with *...*
%%
%% The lifecycle begins with: start handshake_timeout timer, *pre-init*
%%
%% all states, unless specified otherwise:
%%   socket error -> *exit*
%%   socket close -> *throw*
%%   writer send failure -> *throw*
%%   forced termination -> *exit*
%%   handshake_timeout -> *throw*
%% pre-init:
%%   receive protocol header -> send connection.start, *starting*
%% starting:
%%   receive connection.start_ok -> send connection.tune, *tuning*
%% tuning:
%%   receive connection.tune_ok -> start heartbeats, *opening*
%% opening:
%%   receive connection.open -> send connection.open_ok, *running*
%% running:
%%   receive connection.close ->
%%     tell channels to terminate gracefully
%%     if no channels then send connection.close_ok, start
%%        terminate_connection timer, *closed*
%%     else *closing*
%%   forced termination
%%   -> wait for channels to terminate forcefully, start
%%      terminate_connection timer, send close, *exit*
%%   channel exit with hard error
%%   -> log error, wait for channels to terminate forcefully, start
%%      terminate_connection timer, send close, *closed*
%%   channel exit with soft error
%%   -> log error, start terminate_channel timer, mark channel as
%%      closing, *running*
%%   terminate_channel timeout -> remove 'closing' mark, *running*
%%   handshake_timeout -> ignore, *running*
%%   heartbeat timeout -> *throw*
%% closing:
%%   socket close -> *terminate*
%%   receive frame -> ignore, *closing*
%%   terminate_channel timeout -> remove 'closing' mark, *closing*
%%   handshake_timeout -> ignore, *closing*
%%   heartbeat timeout -> *throw*
%%   channel exit with hard error
%%   -> log error, wait for channels to terminate forcefully, start
%%      terminate_connection timer, send close, *closed*
%%   channel exit with soft error
%%   -> log error, start terminate_channel timer, mark channel as
%%      closing
%%      if last channel to exit then send connection.close_ok,
%%         start terminate_connection timer, *closed*
%%      else *closing*
%%   channel exits normally
%%   -> if last channel to exit then send connection.close_ok,
%%      start terminate_connection timer, *closed*
%% closed:
%%   socket close -> *terminate*
%%   receive connection.close_ok -> self() ! terminate_connection,
%%     *closed*
%%   receive frame -> ignore, *closed*
%%   terminate_connection timeout -> *terminate*
%%   terminate_channel timeout -> remove 'closing' mark, *closed*
%%   handshake_timeout -> ignore, *closed*
%%   heartbeat timeout -> *throw*
%%   channel exit -> log error, *closed*
%%
%%
%% TODO: refactor the code so that the above is obvious

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(info/1 :: (pid()) -> [info()]).
-spec(info/2 :: (pid(), [info_key()]) -> [info()]).

-endif.

%%--------------------------------------------------------------------------

start_link() ->
    {ok, proc_lib:spawn_link(?MODULE, init, [self()])}.

init(Parent) ->
    Deb = sys:debug_options([]),
    receive
        {go, Sock} -> start_connection(Parent, Deb, Sock)
    end.

system_continue(Parent, Deb, State) ->
    ?MODULE:mainloop(Parent, Deb, State).

system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

info(Pid) ->
    gen_server:call(Pid, info, infinity).

info(Pid, Items) ->
    case gen_server:call(Pid, {info, Items}, infinity) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

setup_profiling() ->
    Value = rabbit_misc:get_config(profiling_enabled, false),
    case Value of
        once ->
            rabbit_log:info("Enabling profiling for this connection, "
                            "and disabling for subsequent.~n"),
            rabbit_misc:set_config(profiling_enabled, false),
            fprof:trace(start);
        true ->
            rabbit_log:info("Enabling profiling for this connection.~n"),
            fprof:trace(start);
        false ->
            ok
    end,
    Value.

teardown_profiling(Value) ->
    case Value of
        false ->
            ok;
        _ ->
            rabbit_log:info("Completing profiling for this connection.~n"),
            fprof:trace(stop),
            fprof:profile(),
            fprof:analyse([{dest, []}, {cols, 100}])
    end.

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

peername(Sock) ->   
    try
        {Address, Port} = inet_op(fun () -> inet:peername(Sock) end),
        AddressS = inet_parse:ntoa(Address),
        {AddressS, Port}
    catch
        Ex -> rabbit_log:error("error on TCP connection ~p:~p~n",
                               [self(), Ex]),
              rabbit_log:info("closing TCP connection ~p", [self()]),
              exit(normal)
    end.

start_connection(Parent, Deb, ClientSock) ->
    process_flag(trap_exit, true),
    {PeerAddressS, PeerPort} = peername(ClientSock),
    ProfilingValue = setup_profiling(),
    try 
        rabbit_log:info("starting TCP connection ~p from ~s:~p~n",
                        [self(), PeerAddressS, PeerPort]),
        erlang:send_after(?HANDSHAKE_TIMEOUT * 1000, self(),
                          handshake_timeout),
        mainloop(Parent, Deb, switch_callback(
                                #v1{sock = ClientSock,
                                    connection = #connection{
                                      user = none,
                                      timeout_sec = ?HANDSHAKE_TIMEOUT,
                                      frame_max = ?FRAME_MIN_SIZE,
                                      vhost = none},
                                    callback = uninitialized_callback,
                                    recv_ref = none,
                                    connection_state = pre_init},
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
        teardown_profiling(ProfilingValue)
    end,
    done.

mainloop(Parent, Deb, State = #v1{sock= Sock, recv_ref = Ref}) ->
    %%?LOGDEBUG("Reader mainloop: ~p bytes available, need ~p~n", [HaveBytes, WaitUntilNBytes]),
    receive
        {inet_async, Sock, Ref, {ok, Data}} ->
            {State1, Callback1, Length1} =
                handle_input(State#v1.callback, Data,
                             State#v1{recv_ref = none}),
            mainloop(Parent, Deb,
                     switch_callback(State1, Callback1, Length1));
        {inet_async, Sock, Ref, {error, closed}} ->
            if State#v1.connection_state =:= closed ->
                    State;
               true ->
                    throw(connection_closed_abruptly)
            end;
        {inet_async, Sock, Ref, {error, Reason}} ->
            throw({inet_error, Reason});
        {'EXIT', Parent, Reason} ->
            if State#v1.connection_state =:= running ->
                    send_exception(
                      State, 0,
                      {amqp, connection_forced,
                       io_lib:format(
                         "broker forced connection closure with reason '~w'",
                         [Reason]), none});
               true -> ok
            end,
            %% this is what we are expected to do according to 
            %% http://www.erlang.org/doc/man/sys.html
            %%
            %% If we wanted to be *really* nice we should wait for a
            %% while for clients to close the socket at their end,
            %% just as we do in the ordinary error case. However,
            %% since this termination is initiated by our parent it is
            %% probably more important to exit quickly.
            exit(Reason);
        {'EXIT', _Pid, E = {writer, send_failed, _Error}} ->
            throw(E);
        {channel_exit, Channel, Reason} ->
            mainloop(Parent, Deb, handle_channel_exit(Channel, Reason, State));
        {'EXIT', Pid, Reason} ->
            mainloop(Parent, Deb, handle_dependent_exit(Pid, Reason, State));
        {terminate_channel, Channel, Ref1} ->
            mainloop(Parent, Deb, terminate_channel(Channel, Ref1, State));
        terminate_connection ->
            State;
        handshake_timeout ->
            if State#v1.connection_state =:= running orelse
               State#v1.connection_state =:= closing orelse
               State#v1.connection_state =:= closed ->
                    mainloop(Parent, Deb, State);
               true ->
                    throw({handshake_timeout, State#v1.callback})
            end;
        timeout ->
            throw({timeout, State#v1.connection_state});
        {'$gen_call', From, info} ->
            gen_server:reply(From, infos(?INFO_KEYS, State)),
            mainloop(Parent, Deb, State);
        {'$gen_call', From, {info, Items}} ->
            gen_server:reply(From, try {ok, infos(Items, State)}
                                   catch Error -> {error, Error}
                                   end),
            mainloop(Parent, Deb, State);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From,
                                  Parent, ?MODULE, Deb, State);
        Other ->
            %% internal error -> something worth dying for
            exit({unexpected_message, Other})
    end.

switch_callback(OldState, NewCallback, Length) ->
    Ref = inet_op(fun () -> prim_inet:async_recv(
                              OldState#v1.sock, Length, -1) end),
    OldState#v1{callback = NewCallback,
                recv_ref = Ref}.

close_connection(State = #v1{connection = #connection{
                               timeout_sec = TimeoutSec}}) ->
    %% We terminate the connection after the specified interval, but
    %% no later than ?CLOSING_TIMEOUT seconds.
    TimeoutMillisec =
        1000 * if TimeoutSec > 0 andalso
                  TimeoutSec < ?CLOSING_TIMEOUT -> TimeoutSec;
                  true -> ?CLOSING_TIMEOUT
               end,
    erlang:send_after(TimeoutMillisec, self(), terminate_connection),
    State#v1{connection_state = closed}.

close_channel(Channel, State) ->
    Ref = make_ref(),
    TRef = erlang:send_after(1000 * ?CHANNEL_CLOSING_TIMEOUT,
                             self(),
                             {terminate_channel, Channel, Ref}),
    put({closing_channel, Channel}, {Ref, TRef}),
    State.

terminate_channel(Channel, Ref, State) ->
    case get({closing_channel, Channel}) of
        undefined -> ok; %% got close_ok in the meantime
        {Ref, _}  -> erase({closing_channel, Channel}),
                     ok;
        {_Ref, _} -> ok %% got close_ok, and have new closing channel
    end,
    State.

handle_channel_exit(Channel, Reason, State) ->
    %% We remove the channel from the inbound map only. That allows
    %% the channel to be re-opened, but also means the remaining
    %% cleanup, including possibly closing the connection, is deferred
    %% until we get the (normal) exit signal.
    erase({channel, Channel}),
    handle_exception(State, Channel, Reason).

handle_dependent_exit(Pid, normal, State) ->
    channel_cleanup(Pid),
    maybe_close(State);
handle_dependent_exit(Pid, Reason, State) ->
    case channel_cleanup(Pid) of
        undefined -> exit({abnormal_dependent_exit, Pid, Reason});
        Channel   -> maybe_close(handle_exception(State, Channel, Reason))
    end.

channel_cleanup(Pid) ->
    case get({chpid, Pid}) of
        undefined ->
            case get({closing_chpid, Pid}) of
                undefined -> undefined;
                {channel, Channel} ->
                    erase({closing_chpid, Pid}),
                    Channel
            end;
        {channel, Channel} ->
            erase({channel, Channel}),
            erase({chpid, Pid}),
            Channel
    end.

all_channels() -> [Pid || {{chpid, Pid},_} <- get()].

terminate_channels() ->
    NChannels = length([exit(Pid, normal) || Pid <- all_channels()]),
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
        {'EXIT', Pid, Reason} ->
            case channel_cleanup(Pid) of
                undefined ->
                    exit({abnormal_dependent_exit, Pid, Reason});
                Channel ->
                    case Reason of
                        normal -> ok;
                        _ ->
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

maybe_close(State = #v1{connection_state = closing}) ->
    case all_channels() of
        [] -> ok = send_on_channel0(
                     State#v1.sock, #'connection.close_ok'{}),
              close_connection(State);
        _  -> State
    end;
maybe_close(State) ->
    State.

handle_frame(Type, 0, Payload, State = #v1{connection_state = CS})
  when CS =:= closing; CS =:= closed ->
    case analyze_frame(Type, Payload) of
        {method, MethodName, FieldsBin} ->
            handle_method0(MethodName, FieldsBin, State);
        _Other -> State
    end;
handle_frame(_Type, _Channel, _Payload, State = #v1{connection_state = CS})
  when CS =:= closing; CS =:= closed ->
    State;
handle_frame(Type, 0, Payload, State) ->
    case analyze_frame(Type, Payload) of
        error     -> throw({unknown_frame, Type, Payload});
        heartbeat -> State;
        trace     -> State;
        {method, MethodName, FieldsBin} ->
            handle_method0(MethodName, FieldsBin, State);
        Other -> throw({unexpected_frame_on_channel0, Other})
    end;
handle_frame(Type, Channel, Payload, State) ->
    case analyze_frame(Type, Payload) of
        error         -> throw({unknown_frame, Type, Payload});
        heartbeat     -> throw({unexpected_heartbeat_frame, Channel});
        trace         -> throw({unexpected_trace_frame, Channel});
        AnalyzedFrame ->
            %%?LOGDEBUG("Ch ~p Frame ~p~n", [Channel, AnalyzedFrame]),
            case get({channel, Channel}) of
                {chpid, ChPid} ->
                    ok = check_for_close(Channel, ChPid, AnalyzedFrame),
                    ok = rabbit_framing_channel:process(ChPid, AnalyzedFrame),
                    State;
                undefined ->
                    case State#v1.connection_state of
                        running -> send_to_new_channel(
                                     Channel, AnalyzedFrame, State),
                                   State;
                        Other   -> throw({channel_frame_while_starting,
                                          Channel, Other, AnalyzedFrame})
                    end
            end
    end.

analyze_frame(?FRAME_METHOD, <<ClassId:16, MethodId:16, MethodFields/binary>>) ->
    {method, rabbit_framing:lookup_method_name({ClassId, MethodId}), MethodFields};
analyze_frame(?FRAME_HEADER, <<ClassId:16, Weight:16, BodySize:64, Properties/binary>>) ->
    {content_header, ClassId, Weight, BodySize, Properties};
analyze_frame(?FRAME_BODY, Body) ->
    {content_body, Body};
analyze_frame(?FRAME_TRACE, _Body) ->
    trace;
analyze_frame(?FRAME_HEARTBEAT, <<>>) ->
    heartbeat;
analyze_frame(_Type, _Body) ->
    error.

handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32>>, State) ->
    %%?LOGDEBUG("Got frame header: ~p/~p/~p~n", [Type, Channel, PayloadSize]),
    {State, {frame_payload, Type, Channel, PayloadSize}, PayloadSize + 1};

handle_input({frame_payload, Type, Channel, PayloadSize}, PayloadAndMarker, State) ->
    case PayloadAndMarker of
        <<Payload:PayloadSize/binary, ?FRAME_END>> ->
            %%?LOGDEBUG("Frame completed: ~p/~p/~p~n", [Type, Channel, Payload]),
            NewState = handle_frame(Type, Channel, Payload, State),
            {NewState, frame_header, 7};
        _ ->
            throw({bad_payload, PayloadAndMarker})
    end;

handle_input(handshake, <<"AMQP",1,1,ProtocolMajor,ProtocolMinor>>,
             State = #v1{sock = Sock, connection = Connection}) ->
    case check_version({ProtocolMajor, ProtocolMinor},
                       {?PROTOCOL_VERSION_MAJOR, ?PROTOCOL_VERSION_MINOR}) of
        true ->
            {ok, Product} = application:get_key(id),
            {ok, Version} = application:get_key(vsn),
            ok = send_on_channel0(
                   Sock,
                   #'connection.start'{
                     version_major = ?PROTOCOL_VERSION_MAJOR,
                     version_minor = ?PROTOCOL_VERSION_MINOR,
                     server_properties =
                     [{list_to_binary(K), longstr, list_to_binary(V)} ||
                         {K, V} <-
                             [{"product",     Product},
                              {"version",     Version},
                              {"platform",    "Erlang/OTP"},
                              {"copyright",   ?COPYRIGHT_MESSAGE},
                              {"information", ?INFORMATION_MESSAGE}]],
                     mechanisms = <<"PLAIN AMQPLAIN">>,
                     locales = <<"en_US">> }),
            {State#v1{connection = Connection#connection{
                                     timeout_sec = ?NORMAL_TIMEOUT},
                      connection_state = starting},
             frame_header, 7};
        false ->
            throw({bad_version, ProtocolMajor, ProtocolMinor})
    end;

handle_input(handshake, Other, #v1{sock = Sock}) ->
    ok = inet_op(fun () -> gen_tcp:send(
                             Sock, <<"AMQP",1,1,
                                    ?PROTOCOL_VERSION_MAJOR,
                                    ?PROTOCOL_VERSION_MINOR>>) end),
    throw({bad_header, Other});

handle_input(Callback, Data, _State) ->
    throw({bad_input, Callback, Data}).

%% the 0-8 spec, confusingly, defines the version as 8-0
adjust_version({8,0})   -> {0,8};
adjust_version(Version) -> Version.
check_version(ClientVersion, ServerVersion) ->
    {ClientMajor, ClientMinor} = adjust_version(ClientVersion),
    {ServerMajor, ServerMinor} = adjust_version(ServerVersion),
    ClientMajor > ServerMajor
        orelse
          (ClientMajor == ServerMajor andalso
           ClientMinor >= ServerMinor).

%%--------------------------------------------------------------------------

handle_method0(MethodName, FieldsBin, State) ->
    try
        handle_method0(rabbit_framing:decode_method_fields(
                         MethodName, FieldsBin),
                       State)
    catch exit:Reason ->
            CompleteReason =
                case Reason of
                    {amqp, Error, Explanation, none} ->
                        {amqp, Error, Explanation, MethodName};
                    OtherReason -> OtherReason
                end,
            case State#v1.connection_state of
                running -> send_exception(State, 0, CompleteReason);
                Other   -> throw({channel0_error, Other, CompleteReason})
            end
    end.
handle_method0(#'connection.start_ok'{mechanism = Mechanism,
                                      response = Response},
               State = #v1{connection_state = starting,
                           connection = Connection,
                           sock = Sock}) ->
    User = rabbit_access_control:check_login(Mechanism, Response),
    ok = send_on_channel0(
           Sock,
           #'connection.tune'{channel_max = 0,
                              %% set to zero once QPid fix their negotiation
                              frame_max = 131072,
                              heartbeat = 0}),
    State#v1{connection_state = tuning,
             connection = Connection#connection{user = User}};
handle_method0(#'connection.tune_ok'{channel_max = _ChannelMax,
                                     frame_max = FrameMax,
                                     heartbeat = ClientHeartbeat},
               State = #v1{connection_state = tuning,
                           connection = Connection,
                           sock = Sock}) ->
    %% if we have a channel_max limit that the client wishes to
    %% exceed, die as per spec.  Not currently a problem, so we ignore
    %% the client's channel_max parameter.
    rabbit_heartbeat:start_heartbeat(Sock, ClientHeartbeat),
    State#v1{connection_state = opening,
             connection = Connection#connection{
                            timeout_sec = ClientHeartbeat,
                            frame_max = FrameMax}};
handle_method0(#'connection.open'{virtual_host = VHostPath,
                                  insist = Insist},
               State = #v1{connection_state = opening,
                           connection = Connection = #connection{
                                          user = User},
                           sock = Sock}) ->
    ok = rabbit_access_control:check_vhost_access(User, VHostPath),
    NewConnection = Connection#connection{vhost = VHostPath},
    KnownHosts = format_listeners(rabbit_networking:active_listeners()),
    Redirects = compute_redirects(Insist),
    if Redirects == [] ->
            ok = send_on_channel0(
                   Sock,
                   #'connection.open_ok'{known_hosts = KnownHosts}),
            State#v1{connection_state = running,
                     connection = NewConnection};
       true ->
            %% FIXME: 'host' is supposed to only contain one
            %% address; but which one do we pick? This is
            %% really a problem with the spec.
            Host = format_listeners(Redirects),
            rabbit_log:info("connection ~p redirecting to ~p~n",
                            [self(), Host]),
            ok = send_on_channel0(
                   Sock,
                   #'connection.redirect'{host = Host,
                                          known_hosts = KnownHosts}),
            close_connection(State#v1{connection = NewConnection})
    end;
handle_method0(#'connection.close'{},
               State = #v1{connection_state = running}) ->
    lists:foreach(fun rabbit_framing_channel:shutdown/1, all_channels()),
    maybe_close(State#v1{connection_state = closing});
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

send_on_channel0(Sock, Method) ->
    ok = rabbit_writer:internal_send_command(Sock, 0, Method).

format_listeners(Listeners) ->
    list_to_binary(
      rabbit_misc:intersperse(
        $,,
        [io_lib:format("~s:~w", [Host, Port]) ||
            #listener{host = Host, port = Port} <- Listeners])).

compute_redirects(true) -> [];
compute_redirects(false) ->
    Node = node(),
    LNode = rabbit_load:pick(),
    if Node == LNode -> [];
       true -> rabbit_networking:node_listeners(LNode)
    end.

%%--------------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(pid, #v1{}) ->
    self();
i(address, #v1{sock = Sock}) ->
    {ok, {A, _}} = inet:sockname(Sock),
    A;
i(port, #v1{sock = Sock}) ->
    {ok, {_, P}} = inet:sockname(Sock),
    P;
i(peer_address, #v1{sock = Sock}) ->
    {ok, {A, _}} = inet:peername(Sock),
    A;
i(peer_port, #v1{sock = Sock}) ->
    {ok, {_, P}} = inet:peername(Sock),
    P;
i(SockStat, #v1{sock = Sock}) when SockStat =:= recv_oct;
                                   SockStat =:= recv_cnt; 
                                   SockStat =:= send_oct;
                                   SockStat =:= send_cnt;
                                   SockStat =:= send_pend ->
    case inet:getstat(Sock, [SockStat]) of
        {ok, [{SockStat, StatVal}]} -> StatVal;
        {error, einval}             -> undefined;
        {error, Error}              -> throw({cannot_get_socket_stats, Error})
    end;
i(state, #v1{connection_state = S}) ->
    S;
i(channels, #v1{}) ->
    length(all_channels());
i(user, #v1{connection = #connection{user = #user{username = Username}}}) ->
    Username;
i(user, #v1{connection = #connection{user = none}}) ->
    none;
i(vhost, #v1{connection = #connection{vhost = VHost}}) ->
    VHost;
i(timeout, #v1{connection = #connection{timeout_sec = Timeout}}) ->
    Timeout;
i(frame_max, #v1{connection = #connection{frame_max = FrameMax}}) ->
    FrameMax;
i(Item, #v1{}) ->
    throw({bad_argument, Item}).

%%--------------------------------------------------------------------------

send_to_new_channel(Channel, AnalyzedFrame, State) ->
    case get({closing_channel, Channel}) of
        undefined ->
            #v1{sock = Sock,
                connection = #connection{
                  frame_max = FrameMax,
                  user = #user{username = Username},
                  vhost = VHost}} = State,
            WriterPid = rabbit_writer:start(Sock, Channel, FrameMax),
            ChPid = rabbit_framing_channel:start_link(
                      fun rabbit_channel:start_link/5,
                      [Channel, self(), WriterPid, Username, VHost]),
            put({channel, Channel}, {chpid, ChPid}),
            put({chpid, ChPid}, {channel, Channel}),
            ok = rabbit_framing_channel:process(ChPid, AnalyzedFrame);
        {_, TRef} ->
            %% According to the spec, after sending a channel.close we
            %% must ignore all frames except channel.close_ok.
            case AnalyzedFrame of
                {method, 'channel.close_ok', _} ->
                    erlang:cancel_timer(TRef),
                    erase({closing_channel, Channel}),
                    ok;
                _Other -> ok
            end
    end.

check_for_close(Channel, ChPid, {method, 'channel.close', _}) ->
    channel_cleanup(ChPid),
    put({closing_chpid, ChPid}, {channel, Channel}),
    ok;
check_for_close(_Channel, _ChPid, _Frame) ->
    ok.

log_channel_error(ConnectionState, Channel, Reason) ->
    rabbit_log:error("connection ~p (~p), channel ~p - error:~n~p~n",
                     [self(), ConnectionState, Channel, Reason]).

handle_exception(State = #v1{connection_state = closed}, Channel, Reason) ->
    log_channel_error(closed, Channel, Reason),
    State;
handle_exception(State = #v1{connection_state = CS}, Channel, Reason) ->
    log_channel_error(CS, Channel, Reason),
    send_exception(State, Channel, Reason).

send_exception(State, Channel, Reason) ->
    {ShouldClose, CloseChannel, CloseMethod} = map_exception(Channel, Reason),
    NewState = case ShouldClose of
                   true  -> terminate_channels(),
                            close_connection(State);
                   false -> close_channel(Channel, State)
               end,
    ok = rabbit_writer:internal_send_command(
           NewState#v1.sock, CloseChannel, CloseMethod),
    NewState.

map_exception(Channel, Reason) ->
    {SuggestedClose, ReplyCode, ReplyText, FailedMethod} =
        lookup_amqp_exception(Reason),
    ShouldClose = SuggestedClose or (Channel == 0),
    {ClassId, MethodId} = case FailedMethod of
                              {_, _} -> FailedMethod;
                              none -> {0, 0};
                              _ -> rabbit_framing:method_id(FailedMethod)
                          end,
    {CloseChannel, CloseMethod} =
        case ShouldClose of
            true -> {0, #'connection.close'{reply_code = ReplyCode,
                                            reply_text = ReplyText,
                                            class_id = ClassId,
                                            method_id = MethodId}};
            false -> {Channel, #'channel.close'{reply_code = ReplyCode,
                                                reply_text = ReplyText,
                                                class_id = ClassId,
                                                method_id = MethodId}}
        end,
    {ShouldClose, CloseChannel, CloseMethod}.

lookup_amqp_exception({amqp, {ShouldClose, Code, Text}, Expl, Method}) ->
    ExplBin = list_to_binary(Expl),
    CompleteTextBin = <<Text/binary, " - ", ExplBin/binary>>,
    SafeTextBin = if size(CompleteTextBin) > 255 ->
                          <<CompleteTextBin:252/binary, "...">>;
                     true ->
                          CompleteTextBin
                  end,
    {ShouldClose, Code, SafeTextBin, Method};
lookup_amqp_exception({amqp, ErrorName, Expl, Method}) ->
    Details = rabbit_framing:lookup_amqp_exception(ErrorName), 
    lookup_amqp_exception({amqp, Details, Expl, Method});
lookup_amqp_exception(Other) ->
    rabbit_log:warning("Non-AMQP exit reason '~p'~n", [Other]),
    {true, ?INTERNAL_ERROR, <<"INTERNAL_ERROR">>, none}.
