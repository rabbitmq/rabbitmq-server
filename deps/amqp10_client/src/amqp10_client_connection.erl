%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_client_connection).

-behaviour(gen_statem).

-include("amqp10_client_internal.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").

%% public API
-export([open/1,
         close/2]).

%% private API
-export([start_link/2,
         socket_ready/2,
         protocol_header_received/5,
         begin_session/1,
         heartbeat/1]).

%% gen_statem callbacks
-export([init/1,
         callback_mode/0,
         terminate/3,
         code_change/4]).

%% gen_statem state callbacks
%% see figure 2.23
-export([expecting_socket/3,
         sasl_hdr_sent/3,
         sasl_hdr_rcvds/3,
         sasl_init_sent/3,
         hdr_sent/3,
         open_sent/3,
         opened/3,
         close_sent/3]).

-export([format_status/1]).

-type milliseconds() :: non_neg_integer().

-type address() :: inet:socket_address() | inet:hostname().

-type encrypted_sasl() :: {plaintext, binary()} | {encrypted, binary()}.
-type decrypted_sasl() :: none | anon | external | {plain, User :: binary(), Pwd :: binary()}.
-type sasl() :: encrypted_sasl() | decrypted_sasl().

-type connection_config() ::
    #{container_id => binary(), % AMQP container id
      hostname => binary(), % the dns name of the target host
      addresses => [address()],
      address => address(),
      port => inet:port_number(),
      tls_opts => {secure_port, [ssl:tls_option()]},
      ws_path => string(),
      ws_opts => gun:opts(),
      notify => pid() | none, % the pid to send connection events to
      notify_when_opened => pid() | none,
      notify_when_closed => pid() | none,
      notify_with_performative => boolean(),
      %% incoming maximum frame size set by our client application
      max_frame_size => pos_integer(), % TODO: constrain to large than 512
      %% outgoing maximum frame size set by AMQP peer in OPEN performative
      outgoing_max_frame_size => pos_integer() | undefined,
      idle_time_out => milliseconds(),
      % set to a negative value to allow a sender to "overshoot" the flow
      % control by this margin
      transfer_limit_margin => 0 | neg_integer(),
      sasl => sasl(),
      properties => amqp10_client_types:properties()
  }.

-record(state,
        {next_channel = 0 :: non_neg_integer(),
         connection_sup :: pid(),
         reader_m_ref :: reference() | undefined,
         sessions_sup :: pid() | undefined,
         pending_session_reqs = [] :: [term()],
         reader :: pid() | undefined,
         socket :: amqp10_client_socket:socket() | undefined,
         idle_time_out :: non_neg_integer() | undefined,
         heartbeat_timer :: timer:tref() | undefined,
         config :: connection_config()
        }).

-export_type([connection_config/0]).

%% -------------------------------------------------------------------
%% Public API.
%% -------------------------------------------------------------------

-spec open(connection_config()) -> supervisor:startchild_ret().
open(Config0) ->
    Config = maps:update_with(sasl, fun maybe_encrypt_sasl/1, Config0),
    %% Start the supervision tree dedicated to that connection. It
    %% starts at least a connection process (the PID we want to return)
    %% and a reader process (responsible for opening and reading the
    %% socket).
    case supervisor:start_child(amqp10_client_sup, [Config]) of
        {ok, ConnSup} ->
            %% We query the PIDs of the connection and reader processes. The
            %% reader process needs to know the connection PID to send it the
            %% socket.
            Children = supervisor:which_children(ConnSup),
            {_, Reader, _, _} = lists:keyfind(reader, 1, Children),
            {_, Connection, _, _} = lists:keyfind(connection, 1, Children),
            {_, SessionsSup, _, _} = lists:keyfind(sessions, 1, Children),
            set_other_procs(Connection, #{sessions_sup => SessionsSup,
                                          reader => Reader}),
            {ok, Connection};
        Error ->
            Error
    end.

-spec close(pid(), {amqp10_client_types:amqp_error()
                   | amqp10_client_types:connection_error(), binary()} | none) -> ok.
close(Pid, Reason) ->
    gen_statem:cast(Pid, {close, Reason}).

-spec maybe_encrypt_sasl(decrypted_sasl()) -> sasl().
maybe_encrypt_sasl(Sasl)
  when Sasl =:= none orelse
       Sasl =:= anon orelse
       Sasl =:= external ->
    Sasl;
maybe_encrypt_sasl(Plain = {plain, _User, _Passwd}) ->
    credentials_obfuscation:encrypt(term_to_binary(Plain)).

-spec maybe_decrypt_sasl(sasl()) -> decrypted_sasl().
maybe_decrypt_sasl(Sasl)
  when Sasl =:= none orelse
       Sasl =:= anon orelse
       Sasl =:= external ->
    Sasl;
maybe_decrypt_sasl(Encrypted) ->
    binary_to_term(credentials_obfuscation:decrypt(Encrypted)).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

start_link(Sup, Config) ->
    gen_statem:start_link(?MODULE, [Sup, Config], []).

set_other_procs(Pid, OtherProcs) ->
    gen_statem:cast(Pid, {set_other_procs, OtherProcs}).

-spec socket_ready(pid(), amqp10_client_socket:socket()) -> ok.
socket_ready(Pid, Socket) ->
    gen_statem:cast(Pid, {socket_ready, Socket}).

-spec protocol_header_received(pid(), 0 | 3, non_neg_integer(),
                               non_neg_integer(), non_neg_integer()) -> ok.
protocol_header_received(Pid, Protocol, Maj, Min, Rev) ->
    gen_statem:cast(Pid, {protocol_header_received, Protocol, Maj, Min, Rev}).

-spec begin_session(pid()) -> supervisor:startchild_ret().
begin_session(Pid) ->
    gen_statem:call(Pid, begin_session, ?TIMEOUT).

heartbeat(Pid) ->
    gen_statem:cast(Pid, heartbeat).

%% -------------------------------------------------------------------
%% gen_statem callbacks.
%% -------------------------------------------------------------------

callback_mode() -> [state_functions].

init([Sup, Config0]) ->
    process_flag(trap_exit, true),
    Config = maps:merge(config_defaults(), Config0),
    {ok, expecting_socket, #state{connection_sup = Sup,
                                  config = Config}}.

expecting_socket(_EvtType, {socket_ready, Socket},
                 State = #state{config = Cfg}) ->
    State1 = State#state{socket = Socket},
    Sasl = credentials_obfuscation:decrypt(maps:get(sasl, Cfg)),
    case Sasl of
        none ->
            ok = amqp10_client_socket:send(Socket, ?AMQP_PROTOCOL_HEADER),
            {next_state, hdr_sent, State1};
        _ ->
            ok = amqp10_client_socket:send(Socket, ?SASL_PROTOCOL_HEADER),
            {next_state, sasl_hdr_sent, State1}
    end;
expecting_socket(_EvtType, {set_other_procs, OtherProcs}, State) ->
    {keep_state, set_other_procs0(OtherProcs, State)};
expecting_socket({call, From}, begin_session,
                  #state{pending_session_reqs = PendingSessionReqs} = State) ->
    %% The caller already asked for a new session but the connection
    %% isn't fully opened. Let's queue this request until the connection
    %% is ready.
    State1 = State#state{pending_session_reqs = [From | PendingSessionReqs]},
    {keep_state, State1}.

sasl_hdr_sent(_EvtType, {protocol_header_received, 3, 1, 0, 0}, State) ->
    {next_state, sasl_hdr_rcvds, State};
sasl_hdr_sent({call, From}, begin_session,
              #state{pending_session_reqs = PendingSessionReqs} = State) ->
    State1 = State#state{pending_session_reqs = [From | PendingSessionReqs]},
    {keep_state, State1};
sasl_hdr_sent(info, {'DOWN', MRef, process, _Pid, _},
              #state{reader_m_ref = MRef}) ->
    {stop, {shutdown, reader_down}}.

sasl_hdr_rcvds(_EvtType, #'v1_0.sasl_mechanisms'{
                            sasl_server_mechanisms = {array, symbol, AvailableMechs}},
               State = #state{config = #{sasl := Sasl}}) ->
    DecryptedSasl = maybe_decrypt_sasl(Sasl),
    OurMech = {symbol, decrypted_sasl_to_mechanism(DecryptedSasl)},
    case lists:member(OurMech, AvailableMechs) of
        true ->
            ok = send_sasl_init(State, DecryptedSasl),
            {next_state, sasl_init_sent, State};
        false ->
            {stop, {sasl_not_supported, DecryptedSasl}, State}
    end;
sasl_hdr_rcvds({call, From}, begin_session,
               #state{pending_session_reqs = PendingSessionReqs} = State) ->
    State1 = State#state{pending_session_reqs = [From | PendingSessionReqs]},
    {keep_state, State1}.

sasl_init_sent(_EvtType, #'v1_0.sasl_outcome'{code = {ubyte, 0}},
               #state{socket = Socket} = State) ->
    ok = amqp10_client_socket:send(Socket, ?AMQP_PROTOCOL_HEADER),
    {next_state, hdr_sent, State};
sasl_init_sent(_EvtType, #'v1_0.sasl_outcome'{code = {ubyte, C}},
               #state{} = State) when C==1;C==2;C==3;C==4 ->
    {stop, sasl_auth_failure, State};
sasl_init_sent({call, From}, begin_session,
               #state{pending_session_reqs = PendingSessionReqs} = State) ->
    State1 = State#state{pending_session_reqs = [From | PendingSessionReqs]},
    {keep_state, State1}.

hdr_sent(_EvtType, {protocol_header_received, 0, 1, 0, 0}, State) ->
    case send_open(State) of
        ok    -> {next_state, open_sent, State};
        Error -> {stop, Error, State}
    end;
hdr_sent(_EvtType, {protocol_header_received, Protocol, Maj, Min,
                                Rev}, State) ->
    logger:warning("Unsupported protocol version: ~b ~b.~b.~b",
                             [Protocol, Maj, Min, Rev]),
    {stop, normal, State};
hdr_sent({call, From}, begin_session,
         #state{pending_session_reqs = PendingSessionReqs} = State) ->
    State1 = State#state{pending_session_reqs = [From | PendingSessionReqs]},
    {keep_state, State1}.

open_sent(_EvtType, #'v1_0.open'{max_frame_size = MaybeMaxFrameSize,
                                 idle_time_out = Timeout} = Open,
          #state{pending_session_reqs = PendingSessionReqs,
                 config = Config} = State0) ->
    State = case Timeout of
                undefined -> State0;
                {uint, T} when T > 0 ->
                    {ok, Tmr} = start_heartbeat_timer(T div 2),
                    State0#state{idle_time_out = T div 2,
                                 heartbeat_timer = Tmr};
                _ -> State0
            end,
    MaxFrameSize = case unpack(MaybeMaxFrameSize) of
                       undefined ->
                           %% default as per 2.7.1
                           ?UINT_MAX;
                       Bytes when is_integer(Bytes) ->
                           Bytes
                   end,
    State1 = State#state{config = Config#{outgoing_max_frame_size => MaxFrameSize}},
    State2 = lists:foldr(
               fun(From, S0) ->
                       {Ret, S2} = handle_begin_session(From, S0),
                       _ = gen_statem:reply(From, Ret),
                       S2
               end, State1, PendingSessionReqs),
    ok = notify_opened(Config, Open),
    {next_state, opened, State2#state{pending_session_reqs = []}};
open_sent({call, From}, begin_session,
          #state{pending_session_reqs = PendingSessionReqs} = State) ->
    State1 = State#state{pending_session_reqs = [From | PendingSessionReqs]},
    {keep_state, State1};
open_sent(_EvtType, {close, Reason}, State) ->
    case send_close(State, Reason) of
        ok ->
            %% "After writing this frame the peer SHOULD continue to read from the connection
            %% until it receives the partner's close frame (in order to guard against
            %% erroneously or maliciously implemented partners, a peer SHOULD implement a
            %% timeout to give its partner a reasonable time to receive and process the close
            %% before giving up and simply closing the underlying transport mechanism)." [§2.4.3]
            {next_state, close_sent, State, {state_timeout, ?TIMEOUT, received_no_close_frame}};
        {error, closed} ->
            {stop, normal, State};
        Error ->
            {stop, Error, State}
    end;
open_sent(info, {'DOWN', MRef, process, _, _},
          #state{reader_m_ref = MRef}) ->
    {stop, {shutdown, reader_down}}.

opened(_EvtType, heartbeat, State = #state{idle_time_out = T}) ->
    ok = send_heartbeat(State),
    {ok, Tmr} = start_heartbeat_timer(T),
    {keep_state, State#state{heartbeat_timer = Tmr}};
opened(_EvtType, {close, Reason}, State) ->
    %% TODO: stop all sessions writing
    %% We could still accept incoming frames (See: 2.4.6)
    case send_close(State, Reason) of
        ok ->
            %% "After writing this frame the peer SHOULD continue to read from the connection
            %% until it receives the partner's close frame (in order to guard against
            %% erroneously or maliciously implemented partners, a peer SHOULD implement a
            %% timeout to give its partner a reasonable time to receive and process the close
            %% before giving up and simply closing the underlying transport mechanism)." [§2.4.3]
            {next_state, close_sent, State, {state_timeout, ?TIMEOUT, received_no_close_frame}};
        {error, closed} ->
            {stop, normal, State};
        Error ->
            {stop, Error, State}
    end;
opened(_EvtType, #'v1_0.close'{} = Close, State = #state{config = Config}) ->
    %% We receive the first close frame, reply and terminate.
    ok = notify_closed(Config, Close),
    case send_close(State, none) of
        ok              -> {stop, normal, State};
        {error, closed} -> {stop, normal, State};
        Error           -> {stop, Error, State}
    end;
opened({call, From}, begin_session, State) ->
    {Ret, State1} = handle_begin_session(From, State),
    {keep_state, State1, [{reply, From, Ret}]};
opened(info, {'DOWN', MRef, process, _, _Info},
       #state{reader_m_ref = MRef, config = Config}) ->
    %% reader has gone down and we are not already shutting down
    ok = notify_closed(Config, shutdown),
    {stop, normal};
opened(_EvtType, Frame, State) ->
    logger:warning("Unexpected connection frame ~tp when in state ~tp ",
                   [Frame, State]),
    keep_state_and_data.

close_sent(_EvtType, heartbeat, _Data) ->
    keep_state_and_data;
close_sent(_EvtType, {'EXIT', _Pid, shutdown}, _Data) ->
    %% monitored processes may exit during closure
    keep_state_and_data;
close_sent(_EvtType, {'DOWN', _Ref, process, ReaderPid, _Reason},
           #state{reader = ReaderPid}) ->
    %% if the reader exits we probably won't receive a close frame
    {stop, normal};
close_sent(_EvtType, #'v1_0.close'{} = Close, #state{config = Config}) ->
    ok = notify_closed(Config, Close),
    {stop, normal};
close_sent(state_timeout, received_no_close_frame, _Data) ->
    {stop, normal};
close_sent(_EvtType, #'v1_0.open'{}, _Data) ->
    %% Transition from CLOSE_PIPE to CLOSE_SENT in figure 2.23.
    keep_state_and_data.

set_other_procs0(OtherProcs, State) ->
    #{sessions_sup := SessionsSup,
      reader := Reader} = OtherProcs,
    ReaderMRef = monitor(process, Reader),
    amqp10_client_frame_reader:set_connection(Reader, self()),
    State#state{sessions_sup = SessionsSup,
                reader_m_ref = ReaderMRef,
                reader = Reader}.

terminate(Reason, _StateName, #state{connection_sup = Sup,
                                     config = Config}) ->
    ok = notify_closed(Config, Reason),
    case Reason of
        normal -> sys:terminate(Sup, normal);
        _      -> ok
    end.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(Context = #{data := ProcState}) ->
    %% Note: Context.state here refers to the gen_statem state name,
    %%       so we need to use Context.data to get #state{}
    Obfuscated = obfuscate_state(ProcState),
    Context#{data => Obfuscated}.


%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

obfuscate_state(State = #state{config = Cfg0}) ->
    Cfg1 = obfuscate_state_config_sasl(Cfg0),
    Cfg2 = obfuscate_state_config_tls_opts(Cfg1),
    State#state{config = Cfg2}.

-spec obfuscate_state_config_sasl(connection_config()) -> connection_config().
obfuscate_state_config_sasl(Cfg) ->
    Sasl0 = maps:get(sasl, Cfg, none),
    Sasl = case Sasl0 of
               {plain, Username, _Password} ->
                   {plain, Username, <<"[redacted]">>};
               Other ->
                   Other
           end,
    Cfg#{sasl => Sasl}.

-spec obfuscate_state_config_tls_opts(connection_config()) -> connection_config().
obfuscate_state_config_tls_opts(Cfg) ->
    TlsOpts0 = maps:get(tls_opts, Cfg, undefined),
    TlsOpts = case TlsOpts0 of
        {secure_port, PropL0} ->
            Obfuscated = proplists:delete(password, PropL0),
            {secure_port, Obfuscated};
        _ ->
            TlsOpts0
    end,
    Cfg#{tls_opts => TlsOpts}.

handle_begin_session({FromPid, _Ref},
                     #state{sessions_sup = Sup, reader = Reader,
                            next_channel = Channel,
                            config = Config} = State) ->
    Ret = supervisor:start_child(Sup, [FromPid, Channel, Reader, Config]),
    State1 = case Ret of
                 {ok, _} -> State#state{next_channel = Channel + 1};
                 _       -> State
             end,
    {Ret, State1}.

send_open(#state{socket = Socket, config = Config0}) ->
    {ok, Product} = application:get_key(description),
    {ok, Version} = application:get_key(vsn),
    Platform = "Erlang/OTP " ++ erlang:system_info(otp_release),
    Props0 = #{<<"product">> => {utf8, list_to_binary(Product)},
               <<"version">> => {utf8, list_to_binary(Version)},
               <<"platform">> => {utf8, list_to_binary(Platform)}},
    Config = maps:update_with(properties,
                              fun(Val) -> maps:merge(Props0, Val) end,
                              Props0,
                              Config0),
    Props = amqp10_client_types:make_properties(Config),
    ContainerId = maps:get(container_id, Config, generate_container_id()),
    IdleTimeOut = maps:get(idle_time_out, Config, 0),
    IncomingMaxFrameSize = maps:get(max_frame_size, Config),
    Open0 = #'v1_0.open'{container_id = {utf8, ContainerId},
                         channel_max = {ushort, 100},
                         idle_time_out = {uint, IdleTimeOut},
                         properties = Props,
                         max_frame_size = {uint, IncomingMaxFrameSize}
                        },
    Open = case Config of
               #{hostname := Hostname} ->
                   Open0#'v1_0.open'{hostname = {utf8, Hostname}};
               _ ->
                   Open0
           end,
    Encoded = amqp10_framing:encode_bin(Open),
    Frame = amqp10_binary_generator:build_frame(0, Encoded),
    ?DBG("CONN <- ~tp", [Open]),
    amqp10_client_socket:send(Socket, Frame).


send_close(#state{socket = Socket}, _Reason) ->
    Close = #'v1_0.close'{},
    Encoded = amqp10_framing:encode_bin(Close),
    Frame = amqp10_binary_generator:build_frame(0, Encoded),
    ?DBG("CONN <- ~tp", [Close]),
    amqp10_client_socket:send(Socket, Frame).

send_sasl_init(State, anon) ->
    Frame = #'v1_0.sasl_init'{mechanism = {symbol, <<"ANONYMOUS">>}},
    send(Frame, 1, State);
send_sasl_init(State, external) ->
    Frame = #'v1_0.sasl_init'{
               mechanism = {symbol, <<"EXTERNAL">>},
               %% "This response is empty when the client is requesting to act
               %% as the identity the server associated with its authentication
               %% credentials."
               %% https://datatracker.ietf.org/doc/html/rfc4422#appendix-A.1
               initial_response = {binary, <<>>}},
    send(Frame, 1, State);
send_sasl_init(State, {plain, User, Pass}) ->
    Response = <<0:8, User/binary, 0:8, Pass/binary>>,
    Frame = #'v1_0.sasl_init'{mechanism = {symbol, <<"PLAIN">>},
                              initial_response = {binary, Response}},
    send(Frame, 1, State).

send(Record, FrameType, #state{socket = Socket}) ->
    Encoded = amqp10_framing:encode_bin(Record),
    Frame = amqp10_binary_generator:build_frame(0, FrameType, Encoded),
    ?DBG("CONN <- ~tp", [Record]),
    amqp10_client_socket:send(Socket, Frame).

send_heartbeat(#state{socket = Socket}) ->
    Frame = amqp10_binary_generator:build_heartbeat_frame(),
    amqp10_client_socket:send(Socket, Frame).

notify_opened(#{notify_when_opened := none}, _) ->
    ok;
notify_opened(#{notify_when_opened := Pid} = Config, Perf)
  when is_pid(Pid) ->
    notify_opened0(Config, Pid, Perf);
notify_opened(#{notify := Pid} = Config, Perf)
  when is_pid(Pid) ->
    notify_opened0(Config, Pid, Perf);
notify_opened(_, _) ->
    ok.

notify_opened0(Config, Pid, Perf) ->
    Evt = case Config of
              #{notify_with_performative := true} ->
                  {opened, Perf};
              _ ->
                  opened
          end,
    Pid ! amqp10_event(Evt),
    ok.

notify_closed(#{notify_when_closed := none}, _Reason) ->
    ok;
notify_closed(#{notify := none}, _Reason) ->
    ok;
notify_closed(#{notify_when_closed := Pid} = Config, Reason)
  when is_pid(Pid) ->
    notify_closed0(Config, Pid, Reason);
notify_closed(#{notify := Pid} = Config, Reason)
  when is_pid(Pid) ->
    notify_closed0(Config, Pid, Reason).

notify_closed0(#{notify_with_performative := true}, Pid, Perf = #'v1_0.close'{}) ->
    Pid ! amqp10_event({closed, Perf}),
    ok;
notify_closed0(_, Pid,  #'v1_0.close'{error = Error}) ->
    Pid ! amqp10_event({closed, translate_err(Error)}),
    ok;
notify_closed0(_, Pid, Reason) ->
    Pid ! amqp10_event({closed, Reason}),
    ok.

start_heartbeat_timer(Timeout) ->
    timer:apply_after(Timeout, ?MODULE, heartbeat, [self()]).

unpack(V) -> amqp10_client_types:unpack(V).

-spec generate_container_id() -> binary().
generate_container_id() ->
    Pre = atom_to_binary(node()),
    Id = bin_to_hex(crypto:strong_rand_bytes(8)),
    <<Pre/binary, <<"_">>/binary, Id/binary>>.

bin_to_hex(Bin) ->
    <<<<if N >= 10 -> N -10 + $a;
           true  -> N + $0 end>>
      || <<N:4>> <= Bin>>.

translate_err(undefined) ->
    none;
translate_err(#'v1_0.error'{condition = Cond, description = Desc}) ->
    Err =
        case Cond of
            ?V_1_0_AMQP_ERROR_INTERNAL_ERROR -> internal_error;
            ?V_1_0_AMQP_ERROR_NOT_FOUND -> not_found;
            ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS -> unauthorized_access;
            ?V_1_0_AMQP_ERROR_DECODE_ERROR -> decode_error;
            ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED -> resource_limit_exceeded;
            ?V_1_0_AMQP_ERROR_NOT_ALLOWED -> not_allowed;
            ?V_1_0_AMQP_ERROR_INVALID_FIELD -> invalid_field;
            ?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED -> not_implemented;
            ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED -> resource_locked;
            ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED -> precondition_failed;
            ?V_1_0_AMQP_ERROR_RESOURCE_DELETED -> resource_deleted;
            ?V_1_0_AMQP_ERROR_ILLEGAL_STATE -> illegal_state;
            ?V_1_0_AMQP_ERROR_FRAME_SIZE_TOO_SMALL -> frame_size_too_small;
            ?V_1_0_CONNECTION_ERROR_CONNECTION_FORCED -> forced;
            ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR -> framing_error;
            ?V_1_0_CONNECTION_ERROR_REDIRECT -> redirect;
            _ -> Cond
        end,
    {Err, unpack(Desc)}.

amqp10_event(Evt) ->
    {amqp10_event, {connection, self(), Evt}}.

decrypted_sasl_to_mechanism(anon) ->
    <<"ANONYMOUS">>;
decrypted_sasl_to_mechanism(external) ->
    <<"EXTERNAL">>;
decrypted_sasl_to_mechanism({plain, _, _}) ->
    <<"PLAIN">>.

config_defaults() ->
    #{sasl => none,
      transfer_limit_margin => 0,
      %% 1 MB
      max_frame_size => 1_048_576}.
