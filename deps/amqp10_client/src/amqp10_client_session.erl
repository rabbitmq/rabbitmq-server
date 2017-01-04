-module(amqp10_client_session).

-behaviour(gen_fsm).

-include("amqp10_client.hrl").
-include("rabbit_amqp1_0_framing.hrl").

%% Public API.
-export(['begin'/1,
         'end'/1,
         attach/5,
         transfer/3
        ]).

%% Private API.
-export([start_link/2,
         socket_ready/2
        ]).

%% gen_fsm callbacks.
-export([init/1,
         unmapped/2,
         begin_sent/2,
         begin_sent/3,
         mapped/2,
         mapped/3,
         end_sent/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define(MAX_SESSION_WINDOW_SIZE, 65535).
-define(DEFAULT_MAX_HANDLE, 16#ffffffff).

-type link_handle() :: non_neg_integer().
-type link_name() :: binary().
-type role() :: sender | receiver.
-type link_source() :: binary() | undefined.
-type link_target() :: pid() | binary() | undefined.

-record(link,
        {name :: link_name(),
         output_handle :: link_handle(),
         input_handle :: link_handle() | undefined,
         role :: role(),
         source :: link_source(),
         target :: link_target()
         }).

-record(state,
        {channel :: pos_integer(),
         remote_channel :: pos_integer() | undefined,
         next_outgoing_id = 0 :: non_neg_integer(),
         reader :: pid(),
         socket :: gen_tcp:socket() | undefined,
         links = #{} :: #{link_handle() => #link{}},
         link_index = #{} :: #{link_handle() => link_name()},
         next_link_handle = 0 :: link_handle(),
         next_delivery_id = 0 :: non_neg_integer(),
         early_attach_requests = [] :: [term()],
         pending_attach_requests = #{} :: #{link_name() => pid()}
        }).

%% -------------------------------------------------------------------
%% Public API.
%% -------------------------------------------------------------------

-spec 'begin'(pid()) -> supervisor:startchild_ret().

'begin'(Connection) ->
    %% The connection process is responsible for allocating a channel
    %% number and contact the sessions supervisor to start a new session
    %% process.
    amqp10_client_connection:begin_session(Connection).

-spec 'end'(pid()) -> ok.

'end'(Pid) ->
    gen_fsm:send_event(Pid, 'end').

-spec attach(pid(), binary(), role(), #'v1_0.source'{}, #'v1_0.target'{}) ->
    link_handle().
attach(Session, Name, Role, Source, Target) ->
    gen_fsm:sync_send_event(Session, {attach, {Name, Role, Source, Target}}).

-spec transfer(pid(), #'v1_0.transfer'{}, any()) -> ok.
transfer(Session, Transfer, Message) ->
    gen_fsm:sync_send_event(Session, {transfer, {Transfer, Message}}).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

start_link(Channel, Reader) ->
    gen_fsm:start_link(?MODULE, [Channel, Reader], []).

-spec socket_ready(pid(), gen_tcp:socket()) -> ok.

socket_ready(Pid, Socket) ->
    gen_fsm:send_event(Pid, {socket_ready, Socket}).

%% -------------------------------------------------------------------
%% gen_fsm callbacks.
%% -------------------------------------------------------------------

init([Channel, Reader]) ->
    amqp10_client_frame_reader:register_session(Reader, self(), Channel),
    State = #state{channel = Channel, reader = Reader},
    {ok, unmapped, State}.

unmapped({socket_ready, Socket}, State) ->
    State1 = State#state{socket = Socket},
    case send_begin(State1) of
        ok    -> {next_state, begin_sent, State1};
        Error -> {stop, Error, State1}
    end.

begin_sent(#'v1_0.begin'{remote_channel = {ushort, RemoteChannel}},
                      #state{early_attach_requests = EARs} =  State) ->
    error_logger:info_msg("-- SESSION BEGUN (~b <-> ~b) --~n",
                          [State#state.channel, RemoteChannel]),
    State1 = State#state{remote_channel = RemoteChannel},
    State2 = lists:foldr(fun({From, Attach}, S) ->
                                 handle_attach(Attach, From, S) end, State1,
                         EARs),
    {next_state, mapped, State2}.

begin_sent({attach, Attach}, From,
                      #state{early_attach_requests = EARs} = State) ->
    {next_state, expect_begin_frame,
     State#state{early_attach_requests = [{From, Attach} | EARs]}}.

mapped('end', State) ->
    %% We send the first end frame and wait for the reply.
    case send_end(State) of
        ok              -> {next_state, end_sent, State};
        {error, closed} -> {stop, normal, State};
        Error           -> {stop, Error, State}
    end;
mapped(#'v1_0.end'{}, State) ->
    %% We receive the first end frame, reply and terminate.
    _ = send_end(State),
    {stop, normal, State};
mapped(#'v1_0.attach'{name = {utf8, Name}, handle = {uint, Handle}} = Attach,
      #state{links = Links, link_index = LinkIndex,
             pending_attach_requests = PARs} = State) ->
    error_logger:info_msg("ATTACH ~p STATE ~p", [Attach, State]),
    #{Name := From} = PARs,
    #{Name := LinkHandle} = LinkIndex,
    #{LinkHandle := Link0} = Links,
    gen_fsm:reply(From, {ok, LinkHandle}),
    Link = Link0#link{input_handle = Handle},
    {next_state, mapped,
     State#state{links = Links#{LinkHandle := Link},
                 pending_attach_requests = maps:remove(Name, PARs)}};
mapped(Frame, State) ->
    error_logger:info_msg("SESS FRAME ~p STATE ~p", [Frame, State]),
    {next_state, mapped, State}.

mapped({transfer, {#'v1_0.transfer'{handle = {uint, Handle}} = Transfer0,
                  Message}}, _From, #state{links = Links,
                                           next_delivery_id = NDI} = State) ->
    % TODO: handle flow
    #{Handle := _Link} = Links,
    % use the delivery-id as the tag for now
    DeliveryTag = erlang:integer_to_binary(NDI),
    % augment transfer with session stuff
    Transfer = Transfer0#'v1_0.transfer'{delivery_id = {uint, NDI},
                                         delivery_tag = {binary, DeliveryTag}},
    ok = send_transfer(Transfer, Message, State),
    % reply after socket write
    % TODO when using settle = false delay reply until disposition
    {reply, ok, mapped, State#state{next_delivery_id = NDI + 1}};

mapped({attach, Attach}, From, State) ->
    State1 = handle_attach(Attach, From, State),
    {next_state, mapped, State1}.


end_sent(#'v1_0.end'{}, State) ->
    {stop, normal, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(Reason, _StateName, #state{channel = Channel,
                                     remote_channel = RemoteChannel,
                                     reader = Reader}) ->
    case Reason of
        normal -> amqp10_client_frame_reader:unregister_session(
                    Reader, self(), Channel, RemoteChannel);
        _      -> ok
    end,
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

send_begin(#state{socket = Socket,
                  next_outgoing_id = NextOutId} = State) ->
    Begin = #'v1_0.begin'{
               next_outgoing_id = {uint, NextOutId},
               incoming_window = {uint, ?MAX_SESSION_WINDOW_SIZE},
               outgoing_window = {uint, ?MAX_SESSION_WINDOW_SIZE}
              },
    Frame = encode_frame(Begin, State),
    gen_tcp:send(Socket, Frame).

send_end(#state{socket = Socket} = State) ->
    End = #'v1_0.end'{},
    Frame = encode_frame(End, State),
    gen_tcp:send(Socket, Frame).

encode_frame(Record, #state{channel = Channel}) ->
    Encoded = rabbit_amqp1_0_framing:encode_bin(Record),
    rabbit_amqp1_0_binary_generator:build_frame(Channel, Encoded).

send(Record, #state{socket = Socket} = State) ->
    Frame = encode_frame(Record, State),
    gen_tcp:send(Socket, Frame).

encode_transfer_frame(Transfer, Payload0, #state{channel = Channel}) ->
    Encoded = rabbit_amqp1_0_framing:encode_bin(Transfer),
    Payload = rabbit_amqp1_0_framing:encode_bin(Payload0),
    rabbit_amqp1_0_binary_generator:build_frame(Channel, [Encoded, Payload]).

% TODO large messages need to be split into several frames
send_transfer(Transfer, Payload, #state{socket = Socket} = State) ->
    Frame = encode_transfer_frame(Transfer, Payload, State),
    gen_tcp:send(Socket, Frame).

handle_attach({Name, Role, Source, Target}, From,
      #state{next_link_handle = Handle, links = Links,
             pending_attach_requests = PARs,
             link_index = LinkIndex} = State) ->

    % create attach frame
    Attach = #'v1_0.attach'{name = {utf8, Name}, role = Role == receiver,
                            handle = {uint, Handle}, source = Source,
                            initial_delivery_count = {uint, 0},
                            target = Target},
    ok = send(Attach, State),
    {S, T} = case Role of
                 sender -> {undefined, From};
                 receiver -> {Source#'v1_0.source'.address, undefined}
             end,
    Link = #link{name = Name, output_handle = Handle,
                 role = Role, source = S, target = T},

    % stash the From pid
    State#state{links = Links#{Handle => Link},
                 next_link_handle = Handle + 1,
                 pending_attach_requests = PARs#{Name => From},
                 link_index = LinkIndex#{Name => Handle}}.
