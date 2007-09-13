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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_reader).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start_link/0]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/1, mainloop/3]).
-export([analyze_frame/2]).
-export([lookup_amqp_exception/1]).

-import(gen_tcp).
-import(fprof).
-import(inet).
-import(prim_inet).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 1).

%---------------------------------------------------------------------------

-record(v1, {sock, sock_stats,
             connection, callback, recv_ref, connection_state}).

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

setup_profiling() ->
    Value = rabbit_misc:get_config(profiling_enabled, false),
    case Value of
        once ->
            rabbit_log:info("Enabling profiling for this connection, and disabling for subsequent.~n"),
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

start_connection(Parent, Deb, ClientSock) ->
    ProfilingValue = setup_profiling(),
    process_flag(trap_exit, true),
    {ok, {PeerAddress, PeerPort}} = inet:peername(ClientSock),
    try
        case mainloop(Parent, Deb,
                      switch_callback(
                        #v1{sock = ClientSock,
                            sock_stats = none,
                            connection = #connection{
                              user = none,
                              timeout_sec = ?HANDSHAKE_TIMEOUT,
                              heartbeat_sender_pid = none,
                              frame_max = ?FRAME_MIN_SIZE,
                              vhost = none},
                            callback = uninitialized_callback,
                            recv_ref = none,
                            connection_state = pre_init},
                        handshake, 8)) of
            ok -> ok;
            {error, Reason} ->
                release_channel_resources(),
                rabbit_log:error(
                  "Reader error on TCP connection from ~s:~p : ~p~n",
                  [inet_parse:ntoa(PeerAddress), PeerPort, Reason])
        end
    after
        rabbit_log:info("closing TCP connection from ~s:~p~n",
                        [inet_parse:ntoa(PeerAddress), PeerPort]),
        gen_tcp:close(ClientSock)
    end,
    teardown_profiling(ProfilingValue),
    terminate_channels(),
    done.

mainloop(Parent, Deb,
         State = #v1{sock= Sock, recv_ref = Ref,
                     connection = #connection{timeout_sec = TimeoutSec}}) ->
    %%?LOGDEBUG("Reader mainloop: ~p bytes available, need ~p~n", [HaveBytes, WaitUntilNBytes]),
    TimeoutMillisec = 1000 * if
                                 TimeoutSec > 0 ->
                                     TimeoutSec * 2; %% need to wait at least TWO intervals
                                 true ->
                                     ?NORMAL_TIMEOUT
                             end,
    receive
        {inet_async, Sock, Ref, {ok, Data}} ->
            case handle_input(State#v1.callback, Data,
                              State#v1{recv_ref = none}) of
                {ok, {State1, Callback1, Length1}} ->
                    mainloop(Parent, Deb,
                             switch_callback(State1, Callback1, Length1));
                Other -> Other
            end;
        {inet_async, Sock, Ref, {error, closed}} ->
            if State#v1.connection_state == closing ->
                    ok;
               true ->
                    rabbit_log:info("Connection closed abruptly.~n"),
                    ok
            end;
        {inet_async, Sock, Ref, {error, Reason}} ->
            {error, Reason};
        {'EXIT', Pid, Reason} ->
            case handle_dependent_exit(Pid, Reason, State) of
                {ok, State} -> mainloop(Parent, Deb, State);
                Other       -> Other
            end;
        {channel_exception, Channel, Reason} ->
            mainloop(Parent, Deb, handle_exception(State, Channel, Reason));
        {channel_close, Pid} ->
            channel_cleanup(Pid),
            Pid ! handshake,
            mainloop(Parent, Deb, State);
        {tune_connection, Tuning} ->
            ?LOGDEBUG("Reader tune_connection: ~p~n", [Tuning]),
            mainloop(Parent, Deb,
                     State#v1{connection_state = running,
                              connection = Tuning});
        {prepare_close, Pid}  ->
            release_channel_resources(),
            Pid ! handshake,
            mainloop(Parent, Deb, State#v1{connection_state = closing});
        terminate_connection ->
            ok;
        {system, From, Request} ->
            sys:handle_system_msg(Request, From,
                                  Parent, ?MODULE, Deb, State);
        Other ->
            %% internal error -> something worth dying for
            exit({error, {unexpected_message, Other}})
    after TimeoutMillisec ->
            %% Note that we don't care if this fails with {error, ...}
            SocketStats = prim_inet:getstat(Sock, [send_oct, recv_oct]),
            %% We don't time out if any data has been transmitted
            %% since the last time-out. Thus the effective time-out
            %% may be up to twice as long as it ought to be, but
            %% that's ok.
            %% NB: we take into account both incoming and outgoing
            %% data, as per a comment in amqp0-9.xml:
            %% <quote>
            %% so long as there is activity on a connection - in or
            %% out - both peers assume the connection is active.
            %% </quote>
            if (SocketStats /= State#v1.sock_stats) orelse
               (State#v1.connection_state == closing) orelse
               (State#v1.callback == frame_header andalso TimeoutSec == 0) ->
                    mainloop(Parent, Deb,
                             State#v1{sock_stats = SocketStats});
               true ->
                    {error, {timeout, TimeoutSec, State#v1.callback}}
            end
    end.

switch_callback(OldState, NewCallback, Length) ->
    {ok, Ref} = prim_inet:async_recv(OldState#v1.sock, Length, -1),
    OldState#v1{callback = NewCallback,
                recv_ref = Ref}.

closing(State = #v1{connection = #connection{timeout_sec = TimeoutSec}}) ->
    %% We terminate the connection after the specified heartbeat
    %% interval, but no later than ?CLOSING_TIMEOUT seconds.
    TimeoutMillisec =
        1000 * if TimeoutSec > 0 andalso TimeoutSec < ?CLOSING_TIMEOUT ->
                       TimeoutSec;
                  true -> ?CLOSING_TIMEOUT
               end,
    erlang:send_after(TimeoutMillisec, self(), terminate_connection),
    State#v1{connection_state = closing}.

handle_dependent_exit(Pid, Reason, #v1{connection_state = starting}) ->
    {error, {dependent_exit_during_startup, Pid, Reason}};
handle_dependent_exit(Pid, normal, State) ->
    case channel_cleanup(Pid) of
        0      -> {ok, closing(State)};
        _Other -> {ok, State}
    end;
handle_dependent_exit(Pid, Reason, State) ->
    case channel_cleanup(Pid) of
        undefined -> {error, {abnormal_dependent_exit, Pid, Reason}};
        Channel   -> {ok, handle_exception(State, Channel, Reason)}
    end.

channel_cleanup(Pid) ->
    case get({chpid, Pid}) of
        undefined -> undefined;
        {channel, Channel} ->
            erase({channel, Channel}),
            erase({chpid, Pid}),
            rabbit_realm:exit_realms(Pid),
            Channel
    end.

release_channel_resources() ->
    lists:foreach(fun release_channel_resources/1, get()).

release_channel_resources({{chpid, Pid}, _}) ->
    rabbit_realm:exit_realms(Pid);
release_channel_resources(_) -> ok.

terminate_channels() ->
    lists:foreach(fun terminate_channel/1, get()).

terminate_channel({{chpid, Pid}, _}) ->
    Pid ! terminate;
terminate_channel(_) -> ok.

handle_frame(Type, 0, Payload, #v1{connection_state = closing} = State) ->
    case analyze_frame(Type, Payload) of
        {method, 'connection.close_ok', _} ->
            self() ! terminate_connection;
        _Other -> ok
    end,
    {ok, State};
handle_frame(_Type, _Channel, _Payload, #v1{connection_state = closing} = State) ->
    {ok, State};
handle_frame(Type, Channel, Payload, #v1{connection_state = ConnectionState} = State) ->
    case analyze_frame(Type, Payload) of
        error ->
            {error, {unknown_frame, Type, Payload}};
        heartbeat when Channel /= 0 ->
            {error, {unexpected_heartbeat_frame, Channel}};
        heartbeat ->
            %%?LOGDEBUG0("Heartbeat from client~n"),
            {ok, State};
        trace when Channel /= 0 ->
            {error, {unexpected_trace_frame, Channel}};
        trace ->
            {ok, State};
        AnalyzedFrame ->
            %%?LOGDEBUG("Ch ~p Frame ~p~n", [Channel, AnalyzedFrame]),
            case get({channel, Channel}) of
                {chpid, ChPid} ->
                    ChPid ! {frame, Channel, AnalyzedFrame},
                    {ok, State};
                undefined ->
                    case ConnectionState of
                        running ->
                            {State1, ChPid} = open_channel(Channel, rabbit_framing_channel, start, State),
                            ChPid ! {frame, Channel, AnalyzedFrame},
                            {ok, State1};
                        _ ->
                            {ok, handle_exception(State, Channel, channel_error)}
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
    {ok, {State,
          {frame_payload, Type, Channel, PayloadSize}, PayloadSize + 1}};

handle_input({frame_payload, Type, Channel, PayloadSize}, PayloadAndMarker, State) ->
    case PayloadAndMarker of
        <<Payload:PayloadSize/binary, ?FRAME_END>> ->
            %%?LOGDEBUG("Frame completed: ~p/~p/~p~n", [Type, Channel, Payload]),
            case handle_frame(Type, Channel, Payload, State) of
                {ok, State1} -> {ok, {State1, frame_header, 7}};
                Other -> Other
            end;
        _ ->
            {error, {bad_payload, PayloadAndMarker}}
    end;

handle_input(handshake, <<"AMQP",1,1,ProtocolMajor,ProtocolMinor>>, State) ->
    case check_version({ProtocolMajor, ProtocolMinor},
                       {?PROTOCOL_VERSION_MAJOR, ?PROTOCOL_VERSION_MINOR}) of
        true ->
            {State1, _ChPid} = open_channel(0, rabbit_channel0, start, State),
            {ok, {State1#v1{connection = (State1#v1.connection)#connection{timeout_sec = ?NORMAL_TIMEOUT},
                            connection_state = starting},
                  frame_header, 7}};
        false ->
            {error, {bad_version, {ProtocolMajor, ProtocolMinor}}}
    end;

handle_input(handshake, Other, #v1{sock = Sock}) ->
    ok = gen_tcp:send(Sock, <<"AMQP",1,1,?PROTOCOL_VERSION_MAJOR,?PROTOCOL_VERSION_MINOR>>),
    {error, {bad_header, Other}};

handle_input(Callback, Data, _State) ->
    {error, {bad_input, Callback, Data}}.

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

open_channel(Channel, Mod, Fun, State) ->
    ChPid = spawn_link(Mod, Fun, [self(), Channel, State#v1.sock, State#v1.connection]),
    put({channel, Channel}, {chpid, ChPid}),
    put({chpid, ChPid}, {channel, Channel}),
    {State, ChPid}.

%---------------------------------------------------------------------------

handle_exception(State = #v1{connection_state = closing}, Channel, Reason) ->
    rabbit_log:info("Reader in closing state saw channel ~p EXIT: ~p~n",
                    [Channel, Reason]),
    State;
handle_exception(State, Channel, Reason) ->
    case send_exception(State#v1.sock, Channel, Reason) of
        true -> release_channel_resources(),
                closing(State);
        false -> State
    end.

send_exception(_Sock, _Channel, normal) ->
    % Normal shutdown of a channel. We assume any necessary messaging has already been done.
    %?LOGDEBUG("Orderly shutdown of channel ~p~n", [Channel]),
    false;
send_exception(Sock, Channel, Reason) ->
    rabbit_log:info("Sending exception: Channel ~p, Reason ~p~n", [Channel, Reason]),
    {SuggestedClose, ReplyCode, ReplyText, FailedMethod} = lookup_amqp_exception(Reason),
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
    ok = rabbit_writer:internal_send_command(Sock, CloseChannel, CloseMethod),
    ShouldClose.

lookup_amqp_exception({amqp, {ShouldClose, Code, Text}, Method})
  when is_boolean(ShouldClose) andalso is_number(Code) andalso is_binary(Text) ->
    {ShouldClose, Code, Text, Method};
lookup_amqp_exception({amqp, A, Method}) ->
    {ShouldClose, Code, Text} = rabbit_framing:lookup_amqp_exception(A),
    {ShouldClose, Code, Text, Method};
lookup_amqp_exception(Other) ->
    rabbit_log:warning("Non-AMQP exit reason '~p'~n", [Other]),
    {true, ?INTERNAL_ERROR, <<"INTERNAL_ERROR">>, none}.
