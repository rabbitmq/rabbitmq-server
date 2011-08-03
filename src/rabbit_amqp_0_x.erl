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
%% Copyright (c) 2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_amqp_0_x).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([accept_handshake_bytes/1, start_connection/2, send_close_frame/1,
         handle_input/3, assemble_frame/3, assemble_frames/5]).

-export([process_channel_frame/5]). %% used by erlang-client TODO

-rabbit_boot_step({?MODULE,
                   [{description, "AMQP 0-x"},
                    {mfa,         {rabbit_registry, register,
                                   [amqp, <<"0-x">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-behaviour(rabbit_amqp).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 1).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).
-define(SILENT_CLOSE_DELAY, 3).

-define(IS_RUNNING(State),
        (State#v1.connection_state =:= running orelse
         State#v1.connection_state =:= blocking orelse
         State#v1.connection_state =:= blocked)).

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


%% TODO reinstate support for 0-8!

%% The two rules pertaining to version negotiation:
%%
%% * If the server cannot support the protocol specified in the
%% protocol header, it MUST respond with a valid protocol header and
%% then close the socket connection.
%%
%% * The server MUST provide a protocol version that is lower than or
%% equal to that requested by the client in the protocol header.
accept_handshake_bytes(<<"AMQP", 0, 0, 9, 1>>) ->
    true;

%% This is the protocol header for 0-9, which we can safely treat as
%% though it were 0-9-1.
accept_handshake_bytes(<<"AMQP", 1, 1, 0, 9>>) ->
    true;

%% This is what most clients send for 0-8.  The 0-8 spec, confusingly,
%% defines the version as 8-0.
accept_handshake_bytes(<<"AMQP", 1, 1, 8, 0>>) ->
    true;

%% The 0-8 spec as on the AMQP web site actually has this as the
%% protocol header; some libraries e.g., py-amqplib, send it when they
%% want 0-8.
accept_handshake_bytes(<<"AMQP", 1, 1, 9, 1>>) ->
    true;

accept_handshake_bytes(_) ->
    false.

send_close_frame(Sock) ->
    exit(bang).

handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32>>, State) ->
    rabbit_reader:ensure_stats_timer(
      rabbit_reader:switch_callback(State, {frame_payload, Type, Channel, PayloadSize},
                      PayloadSize + 1));

handle_input({frame_payload, Type, Channel, PayloadSize},
             PayloadAndMarker, State) ->
    case PayloadAndMarker of
        <<Payload:PayloadSize/binary, ?FRAME_END>> ->
            rabbit_reader:switch_callback(handle_frame(Type, Channel, Payload, State),
                            frame_header, 7);
        _ ->
            throw({bad_payload, Type, Channel, PayloadSize, PayloadAndMarker})
    end;

handle_input(Callback, Data, _State) ->
    throw({bad_input, Callback, Data}).

assemble_frame(Channel, MethodRecord, Protocol) ->
    ?LOGMESSAGE(out, Channel, MethodRecord, none),
    rabbit_binary_generator:build_simple_method_frame(
      Channel, MethodRecord, Protocol).

assemble_frames(Channel, MethodRecord, Content, FrameMax, Protocol) ->
    ?LOGMESSAGE(out, Channel, MethodRecord, Content),
    MethodName = rabbit_misc:method_record_type(MethodRecord),
    true = Protocol:method_has_content(MethodName), % assertion
    MethodFrame = rabbit_binary_generator:build_simple_method_frame(
                    Channel, MethodRecord, Protocol),
    ContentFrames = rabbit_binary_generator:build_simple_content_frames(
                      Channel, Content, FrameMax, Protocol),
    [MethodFrame | ContentFrames].

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
                            rabbit_reader:channel_cleanup(ChPid),
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

handle_method0(MethodName, FieldsBin,
               State = #v1{connection = #connection{protocol = Protocol}}) ->
    HandleException =
        fun(R) ->
                case ?IS_RUNNING(State) of
                    true  -> rabbit_reader:send_exception(State, 0, R);
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
    ServerFrameMax = rabbit_reader:server_frame_max(),
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
    ok = rabbit_reader:send_on_channel0(Sock, #'connection.open_ok'{}, Protocol),
    State1 = rabbit_reader:internal_conserve_memory(
               rabbit_alarm:register(self(), {?MODULE, conserve_memory, []}),
               State#v1{connection_state = running,
                        connection = NewConnection}),
    rabbit_event:notify(connection_created,
                        [{type, network} |
                         rabbit_reader:infos(?CREATION_EVENT_KEYS, State1)]),
    rabbit_event:if_enabled(StatsTimer,
                            fun() ->rabbit_reader:emit_stats(State1) end),
    State1;
handle_method0(#'connection.close'{}, State) when ?IS_RUNNING(State) ->
    lists:foreach(fun rabbit_channel:shutdown/1, rabbit_reader:all_channels()),
    rabbit_reader:maybe_close(State#v1{connection_state = closing});
handle_method0(#'connection.close'{},
               State = #v1{connection_state = CS,
                           connection = #connection{protocol = Protocol},
                           sock = Sock})
  when CS =:= closing; CS =:= closed ->
    %% We're already closed or closing, so we don't need to cleanup
    %% anything.
    ok = rabbit_reader:send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
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
            ok = rabbit_reader:send_on_channel0(Sock, Secure, Protocol),
            State#v1{auth_state = AuthState1};
        {ok, User} ->
            Tune = #'connection.tune'{channel_max = 0,
                                      frame_max = rabbit_reader:server_frame_max(),
                                      heartbeat = 0},
            ok = rabbit_reader:send_on_channel0(Sock, Tune, Protocol),
            State#v1{connection_state = tuning,
                     connection = Connection#connection{user = User}}
    end.

%% Offer a protocol version to the client.  Connection.start only
%% includes a major and minor version number, Luckily 0-9 and 0-9-1
%% are similar enough that clients will be happy with either.
start_connection(<<"AMQP", 0, 0, 9, 1>>,
                 State = #v1{sock = Sock, connection = Connection}) ->
    {ProtocolMajor, ProtocolMinor, _ProtocolRevision} = {0, 9, 1},
    Protocol = rabbit_framing_amqp_0_9_1,
    Start = #'connection.start'{
      version_major = ProtocolMajor,
      version_minor = ProtocolMinor,
      server_properties = server_properties(Protocol),
      mechanisms = auth_mechanisms_binary(Sock),
      locales = <<"en_US">> },
    ok = rabbit_reader:send_on_channel0(Sock, Start, Protocol),
    rabbit_reader:switch_callback(State#v1{connection = Connection#connection{
                                                          timeout_sec = ?NORMAL_TIMEOUT,
                                                          protocol = Protocol},
                                           connection_state = starting},
                                  frame_header, 7).

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

