-module(amqp10_client_connection).

-behaviour(gen_fsm).

-include("amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

%% Public API.
-export([
         open/1,
         open/2,
         open_sync/1,
         open_sync/2,
         close/1
        ]).

%% Private API.
-export([start_link/2,
         socket_ready/2,
         protocol_header_received/5,
         begin_session/2
        ]).

%% gen_fsm callbacks.
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

%% gen_fsm state callbacks.
-export([expecting_socket/2,
         sasl_hdr_sent/2,
         sasl_hdr_rcvds/2,
         sasl_init_sent/2,
         hdr_sent/2,
         open_sent/2,
         opened/2,
         close_sent/2]).

-type connection_config() ::
    #{container_id => binary(),
      address => inet:socket_address() | inet:hostname(),
      port => inet:port_number(),
      notify => pid(),
      max_frame_size => non_neg_integer(), % TODO constrain to large than 512
      outgoing_max_frame_size => non_neg_integer() | undefined,
      sasl => none | anon | {plain, binary(), binary()} % {plain, User, Pwd}
  }.

-record(state,
        {next_channel = 1 :: pos_integer(),
         connection_sup :: pid(),
         sessions_sup :: pid() | undefined,
         pending_session_reqs = [] :: [term()],
         reader :: pid() | undefined,
         socket :: gen_tcp:socket() | undefined,
         config :: connection_config()
        }).

-export_type([connection_config/0]).

-define(DEFAULT_TIMEOUT, 5000).

%% -------------------------------------------------------------------
%% Public API.
%% -------------------------------------------------------------------


-spec open(
        inet:socket_address() | inet:hostname(),
        inet:port_number()) -> supervisor:startchild_ret().
open(Addr, Port) ->
    open(#{address => Addr, port => Port}).

-spec open_sync(connection_config()) ->
    supervisor:startchild_ret() | connection_timeout.
open_sync(Config) ->
    open_sync(Config, ?DEFAULT_TIMEOUT).

-spec open_sync(connection_config(), timeout()) ->
    supervisor:startchild_ret() | connection_timeout.
open_sync(Config, Timeout) ->
    {ok, Conn} = open(Config),
    receive
        {opened, Conn} -> {ok, Conn}
    after Timeout -> connection_timeout
    end.

-spec open(connection_config()) -> supervisor:startchild_ret().
open(Config) ->
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

-spec close(pid()) -> ok.

close(Pid) ->
    gen_fsm:send_event(Pid, close).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

start_link(Sup, Config) ->
    gen_fsm:start_link(?MODULE, [Sup, Config], []).

set_other_procs(Pid, OtherProcs) ->
    gen_fsm:send_all_state_event(Pid, {set_other_procs, OtherProcs}).

-spec socket_ready(pid(), gen_tcp:socket()) -> ok.

socket_ready(Pid, Socket) ->
    gen_fsm:send_event(Pid, {socket_ready, Socket}).

-spec protocol_header_received(pid(), 0 | 3, non_neg_integer(), non_neg_integer(),
                               non_neg_integer()) -> ok.

protocol_header_received(Pid, Protocol, Maj, Min, Rev) ->
    gen_fsm:send_event(Pid, {protocol_header_received, Protocol, Maj, Min, Rev}).

-spec begin_session(pid(), boolean()) -> supervisor:startchild_ret().

begin_session(Pid, Notify) ->
    gen_fsm:sync_send_all_state_event(Pid, {begin_session, Notify}).

%% -------------------------------------------------------------------
%% gen_fsm callbacks.
%% -------------------------------------------------------------------

init([Sup, Config]) ->
    {ok, expecting_socket, #state{connection_sup = Sup,
                                  config = Config}}.

expecting_socket({socket_ready, Socket}, State = #state{config = Cfg}) ->
    State1 = State#state{socket = Socket},
    case Cfg of
        #{sasl := none} ->
            ok = gen_tcp:send(Socket, ?AMQP_PROTOCOL_HEADER),
            {next_state, hdr_sent, State1};
        _ -> % assume anonymous TODO: PLAIN
            ok = gen_tcp:send(Socket, ?SASL_PROTOCOL_HEADER),
            {next_state, sasl_hdr_sent, State1}
    end.

sasl_hdr_sent({protocol_header_received, 3, 1, 0, 0}, State) ->
    {next_state, sasl_hdr_rcvds, State}.

sasl_hdr_rcvds(#'v1_0.sasl_mechanisms'{
                  sasl_server_mechanisms = {array, _Mechs}}, State) ->
    % TODO validate anon is a returned mechanism
    ok = send_sasl_init(State, <<"ANONYMOUS">>),
    {next_state, sasl_init_sent, State}.

sasl_init_sent(#'v1_0.sasl_outcome'{code = _Code} = O,
               #state{socket = Socket} = State) ->
    % TODO validate anon is a returned mechanism
    error_logger:info_msg("SASL OUTCOME: ~p", [O]),
    ok = gen_tcp:send(Socket, ?AMQP_PROTOCOL_HEADER),
    {next_state, hdr_sent, State}.

hdr_sent({protocol_header_received, 0, 1, 0, 0}, State) ->
    case send_open(State) of
        ok    -> {next_state, open_sent, State};
        Error -> {stop, Error, State}
    end;
hdr_sent({protocol_header_received, Protocol, Maj, Min,
                                Rev}, State) ->
    error_logger:info_msg("Unsupported protocol version: ~b ~b.~b.~b~n",
                          [Protocol, Maj, Min, Rev]),
    {stop, normal, State}.

open_sent(
  #'v1_0.open'{max_frame_size = MFSz, idle_time_out = Timeout},
  #state{pending_session_reqs = PendingSessionReqs, config = Config} = State0) ->
    error_logger:info_msg(
      "-- CONNECTION OPENED -- Max frame size: ~p Idle timeout ~p~n",
      [MFSz, Timeout]),
    State = State0#state{config =
                         Config#{outgoing_max_frame_size => unpack(MFSz)}},
    State3 = lists:foldr(
      fun({From, Notify}, State1) ->
              {Ret, State2} = handle_begin_session(From, Notify, State1),
              _ = gen_fsm:reply(From, Ret),
              State2
      end, State, PendingSessionReqs),
    ok = notify_opened(Config),
    {next_state, opened, State3}.

opened(close, State) ->
    %% We send the first close frame and wait for the reply.
    %% TODO: stop all sessions writing
    %% We could still accept incoming frames (See: 2.4.6)
    case send_close(State) of
        ok              -> {next_state, close_sent, State};
        {error, closed} -> {stop, normal, State};
        Error           -> {stop, Error, State}
    end;
opened(#'v1_0.close'{}, State) ->
    %% We receive the first close frame, reply and terminate.
    _ = send_close(State),
    {stop, normal, State};
opened(Frame, State) ->
    error_logger:info_msg("UNEXPECTED CONNECTION FRAME ~p~n",
                          [Frame]),
    {next_state, opened, State}.

close_sent(#'v1_0.close'{}, State) ->
    {stop, normal, State}.

handle_event({set_other_procs, OtherProcs}, StateName, State) ->
    #{sessions_sup := SessionsSup,
      reader := Reader} = OtherProcs,
    amqp10_client_frame_reader:set_connection(Reader, self()),
    State1 = State#state{sessions_sup = SessionsSup,
                         reader = Reader},
    {next_state, StateName, State1};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event({begin_session, Notify}, From, opened, State) ->
    {Ret, State1} = handle_begin_session(From, Notify, State),
    {reply, Ret, opened, State1};
handle_sync_event({begin_session, Notify}, From, StateName,
                  #state{pending_session_reqs = PendingSessionReqs} = State)
  when StateName =/= close_sent ->
    %% The caller already asked for a new session but the connection
    %% isn't fully opened. Let's queue this request until the connection
    %% is ready.
    State1 = State#state{pending_session_reqs = [{From, Notify} | PendingSessionReqs]},
    {next_state, StateName, State1};
handle_sync_event({begin_session, _Notify}, _, StateName, State) ->
    {reply, {error, connection_closed}, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(Reason, _StateName, #state{connection_sup = Sup}) ->
    case Reason of
        normal -> sys:terminate(Sup, normal);
        _      -> ok
    end,
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

handle_begin_session({FromPid, _Ref}, Notify,
                     #state{sessions_sup = Sup, reader = Reader,
                            next_channel = Channel,
                            config = Config} = State) ->
    Ret = supervisor:start_child(Sup, [FromPid, Notify, Channel, Reader, Config]),
    State1 = case Ret of
                 {ok, _} -> State#state{next_channel = Channel + 1};
                 _       -> State
             end,
    {Ret, State1}.

send_open(#state{socket = Socket, config = Config}) ->
    {ok, Product} = application:get_key(description),
    {ok, Version} = application:get_key(vsn),
    Platform = "Erlang/OTP " ++ erlang:system_info(otp_release),
    Props = {map, [{{symbol, <<"product">>},
                    {utf8, list_to_binary(Product)}},
                   {{symbol, <<"version">>},
                    {utf8, list_to_binary(Version)}},
                   {{symbol, <<"platform">>},
                    {utf8, list_to_binary(Platform)}}
                  ]},
    ContainerId = case Config of
                      #{container_id := Cid} -> Cid;
                      _ -> generate_container_id()
                  end,
    Open0 = #'v1_0.open'{container_id = {utf8, ContainerId},
                         hostname = {utf8, <<"localhost">>},
                         channel_max = {ushort, 100},
                         idle_time_out = {uint, 0},
                         properties = Props},
    Open = case Config of
               #{max_frame_size := MFSz} ->
                   Open0#'v1_0.open'{max_frame_size = {uint, MFSz}};
               _ -> Open0
           end,
    Encoded = rabbit_amqp1_0_framing:encode_bin(Open),
    Frame = rabbit_amqp1_0_binary_generator:build_frame(0, Encoded),
    gen_tcp:send(Socket, Frame).

send_close(#state{socket = Socket}) ->
    Close = #'v1_0.close'{},
    Encoded = rabbit_amqp1_0_framing:encode_bin(Close),
    Frame = rabbit_amqp1_0_binary_generator:build_frame(0, Encoded),
    Ret = gen_tcp:send(Socket, Frame),
    case Ret of
        ok -> _ =
              gen_tcp:shutdown(Socket, write),
              ok;
        _  -> ok
    end,
    Ret.

send_sasl_init(State, Mechanism) ->
    Frame = #'v1_0.sasl_init'{mechanism = {symbol, Mechanism}},
    send(Frame, 1, State).

send(Record, FrameType, #state{socket = Socket}) ->
    Encoded = rabbit_amqp1_0_framing:encode_bin(Record),
    Frame = rabbit_amqp1_0_binary_generator:build_frame(0, FrameType, Encoded),
    gen_tcp:send(Socket, Frame).

notify_opened(#{notify := Pid}) ->
    Pid ! {opened, self()},
    ok;
notify_opened(_Config) -> ok.

unpack(V) -> amqp10_client_types:unpack(V).

-spec generate_container_id() -> binary().
generate_container_id() ->
    Pre = list_to_binary(atom_to_list(node())),
    Id = bin_to_hex(crypto:strong_rand_bytes(8)),
    <<Pre/binary, <<"_">>/binary, Id/binary>>.

bin_to_hex(Bin) ->
    <<<<if N >= 10 -> N -10 + $a;
           true  -> N + $0 end>>
      || <<N:4>> <= Bin>>.
