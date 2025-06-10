%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(amqp10_client_session).

-behaviour(gen_statem).

-include("amqp10_client.hrl").
-include("amqp10_client_internal.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").

%% Public API.
-export(['begin'/1,
         begin_sync/1,
         begin_sync/2,
         'end'/1,
         attach/2,
         detach/2,
         transfer/3,
         disposition/5,
         flow_link/4
        ]).

%% Manual session flow control is currently only used in tests.
-export([flow/3]).

%% Private API
-export([start_link/4,
         socket_ready/2
        ]).

%% gen_statem callbacks
-export([
         init/1,
         terminate/3,
         code_change/4,
         callback_mode/0,
         format_status/1
        ]).

%% gen_statem state callbacks
%% see figure 2.30
-export([
         unmapped/3,
         begin_sent/3,
         mapped/3,
         end_sent/3
        ]).

-import(serial_number,
        [add/2,
         diff/2]).

%% By default, we want to keep the server's remote-incoming-window large at all times.
-define(DEFAULT_MAX_INCOMING_WINDOW, 100_000).
-define(UINT_OUTGOING_WINDOW, {uint, ?UINT_MAX}).
-define(INITIAL_OUTGOING_DELIVERY_ID, ?UINT_MAX).
%% "The next-outgoing-id MAY be initialized to an arbitrary value" [2.5.6]
-define(INITIAL_OUTGOING_TRANSFER_ID, ?UINT_MAX - 1).
%% "Note that, despite its name, the delivery-count is not a count but a
%% sequence number initialized at an arbitrary point by the sender." [2.6.7]
-define(INITIAL_DELIVERY_COUNT, ?UINT_MAX - 2).

-type link_name() :: binary().
%% https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-address-string
%% or
%% https://docs.oasis-open.org/amqp/anonterm/v1.0/anonterm-v1.0.html
-type terminus_address() :: binary() | null.
-type link_role() :: sender | receiver.
-type link_target() :: {pid, pid()} | terminus_address() | undefined.
%% "The locally chosen handle is referred to as the output handle." [2.6.2]
-type output_handle() :: link_handle().
%% "The remotely chosen handle is referred to as the input handle." [2.6.2]
-type input_handle() :: link_handle().

-type terminus_durability() :: none | configuration | unsettled_state.

-type target_def() :: #{address => terminus_address(),
                        durable => terminus_durability()}.
-type source_def() :: #{address => terminus_address(),
                        durable => terminus_durability()}.

-type attach_role() :: {sender, target_def()} | {receiver, source_def(), pid()}.

% http://www.amqp.org/specification/1.0/filters
-type filter() :: #{binary() => #filter{} | binary() | map() | list(binary())}.
-type max_message_size() :: undefined | non_neg_integer().
-type footer_opt() :: crc32 | adler32.

-type attach_args() :: #{name => binary(),
                         role => attach_role(),
                         snd_settle_mode => snd_settle_mode(),
                         rcv_settle_mode => rcv_settle_mode(),
                         filter => filter(),
                         properties => amqp10_client_types:properties(),
                         max_message_size => max_message_size(),
                         handle => output_handle(),
                         footer_opt => footer_opt()
                        }.

-type transfer_error() :: {error,
                           insufficient_credit |
                           remote_incoming_window_exceeded |
                           message_size_exceeded |
                           link_not_found |
                           half_attached}.

-type link_ref() :: #link_ref{}.

-export_type([snd_settle_mode/0,
              rcv_settle_mode/0,
              terminus_durability/0,
              attach_args/0,
              attach_role/0,
              terminus_address/0,
              target_def/0,
              source_def/0,
              filter/0,
              max_message_size/0,
              transfer_error/0]).

-record(link,
        {name :: link_name(),
         ref :: link_ref(),
         state = detached :: detached | attach_sent | attached | detach_sent,
         notify :: pid(),
         output_handle :: output_handle(),
         input_handle :: input_handle() | undefined,
         role :: link_role(),
         target :: link_target(),
         max_message_size :: non_neg_integer() | Unlimited :: undefined,
         delivery_count :: sequence_no() | undefined,
         link_credit = 0 :: non_neg_integer(),
         available = 0 :: non_neg_integer(),
         drain = false :: boolean(),
         partial_transfers :: undefined | {#'v1_0.transfer'{}, [binary()]},
         auto_flow :: never | {RenewWhenBelow :: pos_integer(),
                               Credit :: pos_integer()},
         incoming_unsettled = #{} :: #{delivery_number() => ok},
         footer_opt :: footer_opt() | undefined
        }).

-record(state,
        {channel :: pos_integer(),
         remote_channel :: pos_integer() | undefined,

         %% session flow control, see section 2.5.6
         next_incoming_id :: transfer_number() | undefined,
         %% Can become negative if the peer overshoots our window.
         incoming_window :: integer(),
         auto_flow :: never | {RenewWhenBelow :: pos_integer(),
                               NewWindowSize :: pos_integer()},
         next_outgoing_id = ?INITIAL_OUTGOING_TRANSFER_ID :: transfer_number(),
         remote_incoming_window = 0 :: non_neg_integer(),
         remote_outgoing_window = 0 :: non_neg_integer(),

         reader :: pid(),
         socket :: amqp10_client_socket:socket() | undefined,
         links = #{} :: #{output_handle() => #link{}},
         link_index = #{} :: #{{link_role(), link_name()} => output_handle()},
         link_handle_index = #{} :: #{input_handle() => output_handle()},
         next_link_handle = 0 :: output_handle(),
         early_attach_requests :: [term()],
         connection_config :: amqp10_client_connection:connection_config(),
         outgoing_delivery_id = ?INITIAL_OUTGOING_DELIVERY_ID :: delivery_number(),
         outgoing_unsettled = #{} :: #{delivery_number() => {amqp10_msg:delivery_tag(), Notify :: pid()}},
         notify :: pid()
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

-spec begin_sync(pid()) -> supervisor:startchild_ret().
begin_sync(Connection) ->
    begin_sync(Connection, ?TIMEOUT).

-spec begin_sync(pid(), non_neg_integer()) ->
    supervisor:startchild_ret() | session_timeout.
begin_sync(Connection, Timeout) ->
    {ok, Session} = amqp10_client_connection:begin_session(Connection),
    receive
        {session_begin, Session} -> {ok, Session}
    after Timeout -> session_timeout
    end.

-spec 'end'(pid()) -> ok.
'end'(Pid) ->
    gen_statem:cast(Pid, 'end').

-spec attach(pid(), attach_args()) -> {ok, link_ref()}.
attach(Session, Args) ->
    gen_statem:call(Session, {attach, Args}, ?TIMEOUT).

-spec detach(pid(), output_handle()) -> ok | {error, link_not_found | half_attached}.
detach(Session, Handle) ->
    gen_statem:call(Session, {detach, Handle}, ?TIMEOUT).

-spec transfer(pid(), amqp10_msg:amqp10_msg(), timeout()) ->
    ok | transfer_error().
transfer(Session, Amqp10Msg, Timeout) ->
    [Transfer | Sections] = amqp10_msg:to_amqp_records(Amqp10Msg),
    gen_statem:call(Session, {transfer, Transfer, Sections}, Timeout).

-spec flow(pid(), non_neg_integer(), never | pos_integer()) -> ok.
flow(Session, IncomingWindow, RenewWhenBelow) when
      %% Check that the RenewWhenBelow value make sense.
      RenewWhenBelow =:= never orelse
      is_integer(RenewWhenBelow) andalso
      RenewWhenBelow > 0 andalso
      RenewWhenBelow =< IncomingWindow ->
    gen_statem:cast(Session, {flow_session, IncomingWindow, RenewWhenBelow}).

-spec flow_link(pid(), link_handle(), #'v1_0.flow'{}, never | pos_integer()) -> ok.
flow_link(Session, Handle, Flow, RenewWhenBelow) ->
    gen_statem:cast(Session, {flow_link, Handle, Flow, RenewWhenBelow}).

%% Sending a disposition on a sender link (with receiver-settle-mode = second)
%% is currently unsupported.
-spec disposition(link_ref(), delivery_number(), delivery_number(), boolean(),
                  amqp10_client_types:delivery_state()) -> ok.
disposition(#link_ref{role = receiver,
                      session = Session,
                      link_handle = Handle},
            First, Last, Settled, DeliveryState) ->
    gen_statem:call(Session, {disposition, Handle, First, Last, Settled,
                              DeliveryState}, ?TIMEOUT).


%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

start_link(From, Channel, Reader, ConnConfig) ->
    gen_statem:start_link(?MODULE, [From, Channel, Reader, ConnConfig], []).

-spec socket_ready(pid(), amqp10_client_socket:socket()) -> ok.
socket_ready(Pid, Socket) ->
    gen_statem:cast(Pid, {socket_ready, Socket}).

%% -------------------------------------------------------------------
%% gen_statem callbacks.
%% -------------------------------------------------------------------

callback_mode() -> [state_functions].

init([FromPid, Channel, Reader, ConnConfig]) ->
    process_flag(trap_exit, true),
    amqp10_client_frame_reader:register_session(Reader, self(), Channel),
    State = #state{notify = FromPid,
                   channel = Channel,
                   reader = Reader,
                   connection_config = ConnConfig,
                   incoming_window = ?DEFAULT_MAX_INCOMING_WINDOW,
                   auto_flow = {?DEFAULT_MAX_INCOMING_WINDOW div 2,
                                ?DEFAULT_MAX_INCOMING_WINDOW},
                   early_attach_requests = []},
    {ok, unmapped, State}.

unmapped(cast, {socket_ready, Socket}, State) ->
    State1 = State#state{socket = Socket},
    ok = send_begin(State1),
    {next_state, begin_sent, State1};
unmapped({call, From}, {attach, Attach},
                      #state{early_attach_requests = EARs} = State) ->
    {keep_state,
     State#state{early_attach_requests = [{From, Attach} | EARs]}}.

begin_sent(cast, #'v1_0.begin'{remote_channel = {ushort, RemoteChannel},
                               next_outgoing_id = {uint, NOI},
                               incoming_window = {uint, InWindow},
                               outgoing_window = {uint, OutWindow}} = Begin,
           #state{early_attach_requests = EARs} = State) ->

    State1 = State#state{remote_channel = RemoteChannel},
    State2 = lists:foldr(fun({From, Attach}, S) ->
                                 {S2, H} = send_attach(fun send/2, Attach, From, S),
                                 gen_statem:reply(From, {ok, H}),
                                 S2
                         end, State1, EARs),

    ok = notify_session_begun(Begin, State2),

    {next_state, mapped, State2#state{early_attach_requests = [],
                                      next_incoming_id = NOI,
                                      remote_incoming_window = InWindow,
                                      remote_outgoing_window = OutWindow}};
begin_sent({call, From}, {attach, Attach},
           #state{early_attach_requests = EARs} = State) ->
    {keep_state,
     State#state{early_attach_requests = [{From, Attach} | EARs]}}.

mapped(cast, 'end', State) ->
    %% We send the first end frame and wait for the reply.
    send_end(State),
    {next_state, end_sent, State};
mapped(cast, {flow_link, OutHandle, Flow0, RenewWhenBelow}, State0) ->
    State = send_flow_link(OutHandle, Flow0, RenewWhenBelow, State0),
    {keep_state, State};
mapped(cast, {flow_session, IncomingWindow, RenewWhenBelow}, State0) ->
    AutoFlow = case RenewWhenBelow of
                   never -> never;
                   _ -> {RenewWhenBelow, IncomingWindow}
               end,
    State = State0#state{incoming_window = IncomingWindow,
                         auto_flow = AutoFlow},
    send_flow_session(State),
    {keep_state, State};
mapped(cast, #'v1_0.end'{} = End, State) ->
    %% We receive the first end frame, reply and terminate.
    _ = send_end(State),
    % TODO: send notifications for links?
    ok = notify_session_ended(End, State),
    {stop, normal, State};
mapped(cast, #'v1_0.attach'{name = {utf8, Name},
                            initial_delivery_count = IDC,
                            handle = {uint, InHandle},
                            role = PeerRoleBool,
                            max_message_size = MaybeMaxMessageSize} = Attach,
       #state{links = Links, link_index = LinkIndex,
              link_handle_index = LHI} = State0) ->

    OurRoleBool = not PeerRoleBool,
    OurRole = boolean_to_role(OurRoleBool),
    LinkIndexKey = {OurRole, Name},
    #{LinkIndexKey := OutHandle} = LinkIndex,
    #{OutHandle := Link0} = Links,
    ok = notify_link_attached(Link0, Attach, State0),

    {DeliveryCount, MaxMessageSize} =
    case Link0 of
        #link{role = sender = OurRole,
              delivery_count = DC} ->
            MSS = case MaybeMaxMessageSize of
                      {ulong, S} when S > 0 -> S;
                      _ -> undefined
                  end,
            {DC, MSS};
        #link{role = receiver = OurRole,
              max_message_size = MSS} ->
            {unpack(IDC), MSS}
    end,
    Link = Link0#link{state = attached,
                      input_handle = InHandle,
                      delivery_count = DeliveryCount,
                      max_message_size = MaxMessageSize},
    State = State0#state{links = Links#{OutHandle := Link},
                         link_index = maps:remove(LinkIndexKey, LinkIndex),
                         link_handle_index = LHI#{InHandle => OutHandle}},
    {keep_state, State};
mapped(cast, #'v1_0.detach'{handle = {uint, InHandle}} = Detach,
       #state{links = Links, link_handle_index = LHI} = State0) ->
    with_link(InHandle, State0,
              fun (#link{output_handle = OutHandle} = Link, State) ->
                      ok = notify_link_detached(Link, Detach, State),
                      {keep_state,
                       State#state{links = maps:remove(OutHandle, Links),
                                   link_handle_index = maps:remove(InHandle, LHI)}}
              end);
mapped(cast, #'v1_0.flow'{handle = undefined} = Flow, State0) ->
    State = handle_session_flow(Flow, State0),
    {keep_state, State};
mapped(cast, #'v1_0.flow'{handle = {uint, InHandle}} = Flow,
       #state{links = Links} = State0) ->

    State = handle_session_flow(Flow, State0),

    {ok, #link{output_handle = OutHandle} = Link0} =
        find_link_by_input_handle(InHandle, State),

     % TODO: handle `send_flow` return tag
    {ok, Link} = handle_link_flow(Flow, Link0),
    ok = maybe_notify_link_credit(Link0, Link),
    Links1 = Links#{OutHandle := Link},
    State1 = State#state{links = Links1},
    {keep_state, State1};
mapped(cast, {#'v1_0.transfer'{handle = {uint, InHandle},
                               more = true} = Transfer, Payload},
       #state{links = Links} = State0) ->

    {ok, #link{output_handle = OutHandle} = Link} =
        find_link_by_input_handle(InHandle, State0),

    Link1 = append_partial_transfer(Transfer, Payload, Link),

    State = book_partial_transfer_received(
              State0#state{links = Links#{OutHandle => Link1}}),
    {keep_state, State};
mapped(cast, {Transfer0 = #'v1_0.transfer'{handle = {uint, InHandle}},
              Payload0}, State0) ->
    {ok, #link{target = {pid, TargetPid},
               ref = LinkRef,
               incoming_unsettled = Unsettled,
               footer_opt = FooterOpt
              } = Link0} = find_link_by_input_handle(InHandle, State0),

    {Transfer = #'v1_0.transfer'{settled = Settled,
                                 delivery_id = {uint, DeliveryId}},
     Payload, Link1} = complete_partial_transfer(Transfer0, Payload0, Link0),

    Link2 = case Settled of
                true ->
                    Link1;
                _ ->
                    %% "If not set on the first (or only) transfer for a (multi-transfer) delivery,
                    %% then the settled flag MUST be interpreted as being false." [2.7.5]
                    Link1#link{incoming_unsettled = Unsettled#{DeliveryId => ok}}
            end,
    case decode_as_msg(Transfer, Payload, FooterOpt) of
        {ok, Msg} ->
            % link bookkeeping
            % notify when credit is exhausted (link_credit = 0)
            % detach the Link with a transfer-limit-exceeded error code if further
            % transfers are received
            case book_transfer_received(State0, Link2) of
                {ok, Link3, State1} ->
                    % deliver
                    TargetPid ! {amqp10_msg, LinkRef, Msg},
                    State = auto_flow(Link3, State1),
                    {keep_state, State};
                {credit_exhausted, Link3, State} ->
                    TargetPid ! {amqp10_msg, LinkRef, Msg},
                    notify_credit_exhausted(Link3),
                    {keep_state, State};
                {transfer_limit_exceeded, Link3, State} ->
                    logger:warning("transfer_limit_exceeded for link ~tp", [Link3]),
                    Link = detach_with_error_cond(Link3,
                                                  State,
                                                  ?V_1_0_LINK_ERROR_TRANSFER_LIMIT_EXCEEDED,
                                                  undefined),
                    {keep_state, update_link(Link, State)}
            end;
        {checksum_error, Expected, Actual} ->
            Description = lists:flatten(
                            io_lib:format(
                              "~s checksum error: expected ~b, actual ~b",
                              [FooterOpt, Expected, Actual])),
            logger:warning("deteaching link ~tp due to ~s", [Link2, Description]),
            Link = detach_with_error_cond(Link2,
                                          State0,
                                          ?V_1_0_AMQP_ERROR_DECODE_ERROR,
                                          {utf8, unicode:characters_to_binary(Description)}),
            {keep_state, update_link(Link, State0)}
    end;

% role=true indicates the disposition is from a `receiver`. i.e. from the
% clients point of view these are dispositions relating to `sender`links
mapped(cast, #'v1_0.disposition'{role = true,
                                 settled = true,
                                 first = {uint, First},
                                 last = Last0,
                                 state = DeliveryState},
       #state{outgoing_unsettled = Unsettled0} = State) ->
    Last = case Last0 of
               undefined -> First;
               {uint, L} -> L
           end,
    % TODO: no good if the range becomes very large!! refactor
    Unsettled = serial_number:foldl(
                  fun(Id, Acc0) ->
                          case maps:take(Id, Acc0) of
                              {{DeliveryTag, Pid}, Acc} ->
                                  %% TODO: currently all modified delivery states
                                  %% will be translated to the old, `modified` atom.
                                  %% At some point we should translate into the
                                  %% full {modified, bool, bool, map) tuple.
                                  S = translate_delivery_state(DeliveryState),
                                  ok = notify_disposition(Pid, {S, DeliveryTag}),
                                  Acc;
                              error ->
                                  Acc0
                          end
                  end, Unsettled0, First, Last),

    {keep_state, State#state{outgoing_unsettled = Unsettled}};
mapped(cast, Frame, State) ->
    logger:warning("Unhandled session frame ~tp in state ~tp",
                             [Frame, State]),
    {keep_state, State};
mapped({call, From},
       {transfer, _Transfer, _Sections},
       #state{remote_incoming_window = Window})
  when Window =< 0 ->
    {keep_state_and_data, {reply, From, {error, remote_incoming_window_exceeded}}};
mapped({call, From = {Pid, _}},
       {transfer, #'v1_0.transfer'{handle = {uint, OutHandle},
                                   delivery_tag = {binary, DeliveryTag},
                                   settled = false} = Transfer0, Sections},
       #state{outgoing_delivery_id = DeliveryId, links = Links,
              outgoing_unsettled = Unsettled} = State) ->
    case Links of
        #{OutHandle := #link{input_handle = undefined}} ->
            {keep_state_and_data, {reply, From, {error, half_attached}}};
        #{OutHandle := #link{link_credit = LC}} when LC =< 0 ->
            {keep_state_and_data, {reply, From, {error, insufficient_credit}}};
        #{OutHandle := Link = #link{max_message_size = MaxMessageSize,
                                    footer_opt = FooterOpt}} ->
            Transfer = Transfer0#'v1_0.transfer'{delivery_id = uint(DeliveryId)},
            case send_transfer(Transfer, Sections, FooterOpt, MaxMessageSize, State) of
                {ok, NumFrames} ->
                    State1 = State#state{outgoing_unsettled = Unsettled#{DeliveryId => {DeliveryTag, Pid}}},
                    {keep_state, book_transfer_send(NumFrames, Link, State1), {reply, From, ok}};
                Error ->
                    {keep_state_and_data, {reply, From, Error}}
            end;
        _ ->
            {keep_state_and_data, {reply, From, {error, link_not_found}}}

    end;
mapped({call, From},
       {transfer, #'v1_0.transfer'{handle = {uint, OutHandle}} = Transfer0,
        Sections}, #state{outgoing_delivery_id = DeliveryId,
                       links = Links} = State) ->
    case Links of
        #{OutHandle := #link{input_handle = undefined}} ->
            {keep_state_and_data, {reply, From, {error, half_attached}}};
        #{OutHandle := #link{link_credit = LC}} when LC =< 0 ->
            {keep_state_and_data, {reply, From, {error, insufficient_credit}}};
        #{OutHandle := Link = #link{max_message_size = MaxMessageSize,
                                    footer_opt = FooterOpt}} ->
            Transfer = Transfer0#'v1_0.transfer'{delivery_id = uint(DeliveryId)},
            case send_transfer(Transfer, Sections, FooterOpt, MaxMessageSize, State) of
                {ok, NumFrames} ->
                    {keep_state, book_transfer_send(NumFrames, Link, State), {reply, From, ok}};
                Error ->
                    {keep_state_and_data, {reply, From, Error}}
            end;
        _ ->
            {keep_state_and_data, {reply, From, {error, link_not_found}}}
    end;

mapped({call, From},
       {disposition, OutputHandle, First, Last, Settled0, DeliveryState},
       #state{links = Links} = State0) ->
    #{OutputHandle := Link0 = #link{incoming_unsettled = Unsettled0}} = Links,
    Unsettled = serial_number:foldl(fun maps:remove/2, Unsettled0, First, Last),
    Link = Link0#link{incoming_unsettled = Unsettled},
    State1 = State0#state{links = Links#{OutputHandle := Link}},
    State = auto_flow(Link, State1),
    Disposition = #'v1_0.disposition'{
                     role = translate_role(receiver),
                     first = {uint, First},
                     last = {uint, Last},
                     settled = Settled0,
                     state = translate_delivery_state(DeliveryState)},
    Res = send(Disposition, State),
    {keep_state, State, {reply, From, Res}};

mapped({call, From}, {attach, Attach}, State) ->
    {State1, LinkRef} = send_attach(fun send/2, Attach, From, State),
    {keep_state, State1, {reply, From, {ok, LinkRef}}};

mapped({call, From}, Msg, State) ->
    {Reply, State1} = send_detach(fun send/2, Msg, From, State),
    {keep_state, State1, {reply, From, Reply}};

mapped(_EvtType, Msg, _State) ->
    logger:warning("amqp10_session: unhandled msg in mapped state ~W",
                   [Msg, 10]),
    keep_state_and_data.

end_sent(_EvtType, #'v1_0.end'{} = End, State) ->
    ok = notify_session_ended(End, State),
    {stop, normal, State};
end_sent(_EvtType, _Frame, _State) ->
    % just drop frames here
    keep_state_and_data.

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
                  next_outgoing_id = NextOutId,
                  incoming_window = InWin} = State) ->
    Begin = #'v1_0.begin'{next_outgoing_id = uint(NextOutId),
                          incoming_window = uint(InWin),
                          outgoing_window = ?UINT_OUTGOING_WINDOW},
    Frame = encode_frame(Begin, State),
    socket_send(Socket, Frame).

send_end(State) ->
    send_end(State, undefined).

send_end(#state{socket = Socket} = State, Cond) ->
    Err = #'v1_0.error'{condition = Cond},
    End = #'v1_0.end'{error = Err},
    Frame = encode_frame(End, State),
    socket_send(Socket, Frame).

encode_frame(Record, #state{channel = Channel}) ->
    Encoded = amqp10_framing:encode_bin(Record),
    amqp10_binary_generator:build_frame(Channel, Encoded).

send(Record, #state{socket = Socket} = State) ->
    Frame = encode_frame(Record, State),
    socket_send(Socket, Frame).

send_transfer(Transfer0, Sections0, FooterOpt, MaxMessageSize,
              #state{socket = Socket,
                     channel = Channel,
                     connection_config = Config}) ->
    OutMaxFrameSize = maps:get(outgoing_max_frame_size, Config),
    Transfer = amqp10_framing:encode_bin(Transfer0),
    TransferSize = iolist_size(Transfer),
    Sections = encode_sections(Sections0, FooterOpt),
    SectionsBin = iolist_to_binary(Sections),
    if is_integer(MaxMessageSize) andalso
       MaxMessageSize > 0 andalso
       byte_size(SectionsBin) > MaxMessageSize ->
           {error, message_size_exceeded};
       true ->
           % TODO: this does not take the extended header into account
           % see: 2.3
           MaxPayloadSize = OutMaxFrameSize - TransferSize - ?FRAME_HEADER_SIZE,
           Frames = build_frames(Channel, Transfer0, SectionsBin, MaxPayloadSize, []),
           ok = socket_send(Socket, Frames),
           {ok, length(Frames)}
    end.

encode_sections(Sections, undefined) ->
    [amqp10_framing:encode_bin(S) || S <- Sections];
encode_sections(Sections, FooterOpt) ->
    {Bare, NoBare} = lists:partition(fun is_bare_message_section/1, Sections),
    {FooterL, PreBare} = lists:partition(fun(#'v1_0.footer'{}) ->
                                                 true;
                                            (_) ->
                                                 false
                                         end, NoBare),
    PreBareEncoded = [amqp10_framing:encode_bin(S) || S <- PreBare],
    BareEncoded = [amqp10_framing:encode_bin(S) || S <- Bare],
    {Key, Checksum} = case FooterOpt of
                          crc32 ->
                              {<<"x-opt-crc-32">>, erlang:crc32(BareEncoded)};
                          adler32 ->
                              {<<"x-opt-adler-32">>, erlang:adler32(BareEncoded)}
                      end,
    Ann = {{symbol, Key}, {uint, Checksum}},
    Footer = case FooterL of
                 [] ->
                     #'v1_0.footer'{content = [Ann]};
                 [F = #'v1_0.footer'{content = Content}] ->
                     F#'v1_0.footer'{content = [Ann | Content]}
             end,
    FooterEncoded = amqp10_framing:encode_bin(Footer),
    [PreBareEncoded, BareEncoded, FooterEncoded].

is_bare_message_section(#'v1_0.header'{}) ->
    false;
is_bare_message_section(#'v1_0.delivery_annotations'{}) ->
    false;
is_bare_message_section(#'v1_0.message_annotations'{}) ->
    false;
is_bare_message_section(#'v1_0.footer'{}) ->
    false;
is_bare_message_section(_Section) ->
    true.

send_flow_link(OutHandle,
               #'v1_0.flow'{link_credit = {uint, Credit}} = Flow0, RenewWhenBelow,
               #state{links = Links} = State) ->
    AutoFlow = case RenewWhenBelow of
                   never -> never;
                   _ -> {RenewWhenBelow, Credit}
               end,
    #{OutHandle := #link{output_handle = H,
                         role = receiver,
                         delivery_count = DeliveryCount,
                         available = Available} = Link} = Links,
    Flow1 = Flow0#'v1_0.flow'{
                    handle = uint(H),
                    %% "In the event that the receiving link endpoint has not yet seen the
                    %% initial attach frame from the sender this field MUST NOT be set." [2.7.4]
                    delivery_count = maybe_uint(DeliveryCount),
                    available = uint(Available)},
    Flow = set_flow_session_fields(Flow1, State),
    ok = send(Flow, State),
    State#state{links = Links#{OutHandle =>
                               Link#link{link_credit = Credit,
                                         auto_flow = AutoFlow}}}.

send_flow_session(State) ->
    Flow = set_flow_session_fields(#'v1_0.flow'{}, State),
    ok = send(Flow, State).

set_flow_session_fields(Flow, #state{next_incoming_id = NID,
                                     incoming_window = IW,
                                     next_outgoing_id = NOI}) ->
    Flow#'v1_0.flow'{
           %% "This value MUST be set if the peer has received the begin
           %% frame for the session, and MUST NOT be set if it has not." [2.7.4]
           next_incoming_id = maybe_uint(NID),
           %% IncomingWindow0 can be negative when the sending server overshoots our window.
           %% We must set a floor of 0 in the FLOW frame because field incoming-window is an uint.
           incoming_window = uint(max(0, IW)),
           next_outgoing_id = uint(NOI),
           outgoing_window = ?UINT_OUTGOING_WINDOW}.

build_frames(Channel, Trf, Bin, MaxPayloadSize, Acc)
  when byte_size(Bin) =< MaxPayloadSize ->
    T = amqp10_framing:encode_bin(Trf#'v1_0.transfer'{more = false}),
    Frame = amqp10_binary_generator:build_frame(Channel, [T, Bin]),
    lists:reverse([Frame | Acc]);
build_frames(Channel, Trf, Payload, MaxPayloadSize, Acc) ->
    <<Bin:MaxPayloadSize/binary, Rest/binary>> = Payload,
    T = amqp10_framing:encode_bin(Trf#'v1_0.transfer'{more = true}),
    Frame = amqp10_binary_generator:build_frame(Channel, [T, Bin]),
    build_frames(Channel, Trf, Rest, MaxPayloadSize, [Frame | Acc]).

make_source(#{role := {sender, _}}) ->
    #'v1_0.source'{};
make_source(#{role := {receiver, Source, _Pid},
              filter := Filter}) ->
    Durable = translate_terminus_durability(maps:get(durable, Source, none)),
    Dynamic = maps:get(dynamic, Source, false),
    TranslatedFilter = translate_filters(Filter),
    #'v1_0.source'{address = make_address(Source),
                   durable = {uint, Durable},
                   dynamic = Dynamic,
                   filter = TranslatedFilter,
                   capabilities = make_capabilities(Source)}.

make_target(#{role := {receiver, _Source, _Pid}}) ->
    #'v1_0.target'{};
make_target(#{role := {sender, Target}}) ->
    Durable = translate_terminus_durability(maps:get(durable, Target, none)),
    Dynamic = maps:get(dynamic, Target, false),
    #'v1_0.target'{address = make_address(Target),
                   durable = {uint, Durable},
                   dynamic = Dynamic,
                   capabilities = make_capabilities(Target)}.

make_address(#{address := Addr}) ->
    if is_binary(Addr) ->
           {utf8, Addr};
       is_atom(Addr) ->
           Addr
    end.

make_capabilities(#{capabilities := Caps0}) ->
    Caps = [{symbol, C} || C <- Caps0],
    {array, symbol, Caps};
make_capabilities(_) ->
    undefined.

max_message_size(#{max_message_size := Size})
  when is_integer(Size) andalso
       Size > 0 ->
    {ulong, Size};
max_message_size(_) ->
    undefined.

translate_terminus_durability(none) -> 0;
translate_terminus_durability(configuration) -> 1;
translate_terminus_durability(unsettled_state) -> 2.

translate_filters(Filters)
  when map_size(Filters) =:= 0 ->
    undefined;
translate_filters(Filters) ->
    {map, lists:map(
            fun({Name, #filter{descriptor = Desc,
                               value = V}})
                  when is_binary(Name) ->
                    Descriptor = if is_binary(Desc) -> {symbol, Desc};
                                    is_integer(Desc) -> {ulong, Desc}
                                 end,
                    {{symbol, Name}, {described, Descriptor, V}};
               ({<<"apache.org:legacy-amqp-headers-binding:map">> = K, V})
                 when is_map(V) ->
                    %% special case conversion
                    Key = sym(K),
                    Val = translate_legacy_amqp_headers_binding(V),
                    {Key, {described, Key, Val}};
               ({K, V})
                 when is_binary(K) ->
                    Key = {symbol, K},
                    Val = filter_value_type(V),
                    {Key, {described, Key, Val}}
            end, maps:to_list(Filters))}.

filter_value_type(V)
  when is_binary(V) ->
    %% this is clearly not always correct
    {utf8, V};
filter_value_type(V)
  when is_integer(V) andalso V >= 0 ->
    {uint, V};
filter_value_type(VList)
  when is_list(VList) ->
    {list, [filter_value_type(V) || V <- VList]};
filter_value_type({T, _} = V)
  when is_atom(T) ->
    %% looks like an already tagged type, just pass it through
    V.

% https://people.apache.org/~rgodfrey/amqp-1.0/apache-filters.html
translate_legacy_amqp_headers_binding(LegacyHeaders) ->
    {map,
     maps:fold(
       fun(<<"x-match">> = K, <<"any">> = V, Acc) ->
               [{{utf8, K}, {utf8, V}} | Acc];
          (<<"x-match">> = K, <<"all">> = V, Acc) ->
               [{{utf8, K}, {utf8, V}} | Acc];
          (<<"x-", _/binary>>, _, Acc) ->
               Acc;
          (K, V, Acc) ->
               [{{utf8, K}, filter_value_type(V)} | Acc]
       end, [], LegacyHeaders)}.

send_detach(Send, {detach, OutHandle}, _From, State = #state{links = Links}) ->
    case Links of
        #{OutHandle := #link{input_handle = undefined}} ->
            % Link = stash_link_request(Link0, From, Msg),
            % not fully attached yet - stash the request for later processing
            {{error, half_attached}, State};
        #{OutHandle := Link} ->
            Detach = #'v1_0.detach'{handle = uint(OutHandle),
                                    closed = true},
            ok = Send(Detach, State),
            Links1 = Links#{OutHandle => Link#link{state = detach_sent}},
            {ok, State#state{links = Links1}};
        _ ->
            {{error, link_not_found}, State}
    end.

detach_with_error_cond(Link = #link{output_handle = OutHandle}, State, Cond, Description) ->
    Err = #'v1_0.error'{condition = Cond,
                        description = Description},
    Detach = #'v1_0.detach'{handle = uint(OutHandle),
                            closed = true,
                            error = Err},
    ok = send(Detach, State),
    Link#link{state = detach_sent}.

send_attach(Send, #{name := Name, role := RoleTuple} = Args, {FromPid, _},
            #state{next_link_handle = OutHandle0, links = Links,
             link_index = LinkIndex} = State) ->

    Source = make_source(Args),
    Target = make_target(Args),
    Properties = amqp10_client_types:make_properties(Args),

    {LinkTarget, InitialDeliveryCount, MaxMessageSize} =
    case RoleTuple of
        {receiver, _, Pid} ->
            {{pid, Pid}, undefined, max_message_size(Args)};
        {sender, #{address := TargetAddr}} ->
            {TargetAddr, uint(?INITIAL_DELIVERY_COUNT), undefined}
    end,

    {OutHandle, NextLinkHandle} = case Args of
                                      #{handle := Handle} ->
                                          %% Client app provided link handle.
                                          %% Really only meant for integration tests.
                                          {Handle, OutHandle0};
                                      _ ->
                                          {OutHandle0, OutHandle0 + 1}
                                  end,
    Role = element(1, RoleTuple),
    % create attach performative
    Attach = #'v1_0.attach'{name = {utf8, Name},
                            role = role_to_boolean(Role),
                            handle = {uint, OutHandle},
                            source = Source,
                            properties = Properties,
                            initial_delivery_count = InitialDeliveryCount,
                            snd_settle_mode = snd_settle_mode(Args),
                            rcv_settle_mode = rcv_settle_mode(Args),
                            target = Target,
                            max_message_size = MaxMessageSize},
    ok = Send(Attach, State),

    Ref = make_link_ref(Role, self(), OutHandle),
    Link = #link{name = Name,
                 ref = Ref,
                 output_handle = OutHandle,
                 state = attach_sent,
                 role = Role,
                 notify = FromPid,
                 auto_flow = never,
                 target = LinkTarget,
                 delivery_count = unpack(InitialDeliveryCount),
                 max_message_size = unpack(MaxMessageSize),
                 footer_opt = maps:get(footer_opt, Args, undefined)},

    {State#state{links = Links#{OutHandle => Link},
                 next_link_handle = NextLinkHandle,
                 link_index = LinkIndex#{{Role, Name} => OutHandle}}, Ref}.

-spec handle_session_flow(#'v1_0.flow'{}, #state{}) -> #state{}.
handle_session_flow(#'v1_0.flow'{next_incoming_id = MaybeNII,
                                 next_outgoing_id = {uint, NOI},
                                 incoming_window = {uint, InWin},
                                 outgoing_window = {uint, OutWin}},
       #state{next_outgoing_id = OurNOI} = State) ->
    NII = case MaybeNII of
              {uint, N} -> N;
              undefined -> ?INITIAL_OUTGOING_TRANSFER_ID
          end,
    RemoteIncomingWindow = diff(add(NII, InWin), OurNOI), % see: 2.5.6
    State#state{next_incoming_id = NOI,
                remote_incoming_window = RemoteIncomingWindow,
                remote_outgoing_window = OutWin}.


-spec handle_link_flow(#'v1_0.flow'{}, #link{}) -> {ok | send_flow, #link{}}.
handle_link_flow(#'v1_0.flow'{drain = true,
                              link_credit = {uint, TheirCredit}},
                 Link = #link{role = sender,
                              delivery_count = OurDC,
                              available = 0}) ->
    {send_flow, Link#link{link_credit = 0,
                          delivery_count = add(OurDC, TheirCredit)}};
handle_link_flow(#'v1_0.flow'{delivery_count = MaybeTheirDC,
                              link_credit = {uint, TheirCredit}},
                 Link = #link{role = sender,
                              delivery_count = OurDC}) ->
    TheirDC = case MaybeTheirDC of
                  {uint, DC} -> DC;
                  undefined -> ?INITIAL_DELIVERY_COUNT
              end,
    LinkCredit = amqp10_util:link_credit_snd(TheirDC, TheirCredit, OurDC),
    {ok, Link#link{link_credit = LinkCredit}};
handle_link_flow(#'v1_0.flow'{delivery_count = TheirDC,
                              link_credit = {uint, TheirCredit},
                              available = Available,
                              drain = Drain0},
                 Link0 = #link{role = receiver}) ->
    Drain = default(Drain0, false),
    Link = case Drain andalso TheirCredit =:= 0 of
               true ->
                   notify_credit_exhausted(Link0),
                   Link0#link{delivery_count = unpack(TheirDC),
                              link_credit = 0,
                              available = unpack(Available),
                              drain = Drain};
               false ->
                   Link0#link{delivery_count = unpack(TheirDC),
                              available = unpack(Available),
                              drain = Drain}
           end,
    {ok, Link}.

-spec find_link_by_input_handle(input_handle(), #state{}) ->
    {ok, #link{}} | not_found.
find_link_by_input_handle(InHandle, #state{link_handle_index = LHI,
                                           links = Links}) ->
    case LHI of
        #{InHandle := OutHandle} ->
            case Links of
                #{OutHandle := Link} ->
                    {ok, Link};
                _ -> not_found
            end;
        _ -> not_found
    end.

with_link(InHandle, State, Fun) ->
    case find_link_by_input_handle(InHandle, State) of
        {ok, Link} ->
            Fun(Link, State);
        not_found ->
            % end session with errant-link
            ok = send_end(State, ?V_1_0_SESSION_ERROR_ERRANT_LINK),
            {next_state, end_sent, State}
    end.

maybe_uint(undefined) -> undefined;
maybe_uint(Int) -> uint(Int).

uint(Int) -> {uint, Int}.

unpack(X) -> amqp10_client_types:unpack(X).

snd_settle_mode(#{snd_settle_mode := unsettled}) -> {ubyte, 0};
snd_settle_mode(#{snd_settle_mode := settled}) -> {ubyte, 1};
snd_settle_mode(#{snd_settle_mode := mixed}) -> {ubyte, 2};
snd_settle_mode(_) -> undefined.

rcv_settle_mode(#{rcv_settle_mode := first}) -> {ubyte, 0};
rcv_settle_mode(#{rcv_settle_mode := second}) -> {ubyte, 1};
rcv_settle_mode(_) -> undefined.

% certain amqp10 brokers (IBM MQ) return an undefined delivery state
% when the link is detached before settlement is sent
% TODO: work out if we can assume accepted
translate_delivery_state(undefined) -> undefined;
translate_delivery_state(#'v1_0.accepted'{}) -> accepted;
translate_delivery_state(#'v1_0.rejected'{error = undefined}) -> rejected;
translate_delivery_state(#'v1_0.rejected'{error = Error}) -> {rejected, Error};
translate_delivery_state(#'v1_0.modified'{}) -> modified;
translate_delivery_state(#'v1_0.released'{}) -> released;
translate_delivery_state(#'v1_0.received'{}) -> received;
translate_delivery_state(accepted) -> #'v1_0.accepted'{};
translate_delivery_state(rejected) -> #'v1_0.rejected'{};
translate_delivery_state(modified) -> #'v1_0.modified'{};
translate_delivery_state({modified,
                          DeliveryFailed,
                          UndeliverableHere,
                          MessageAnnotations}) ->
    MA = translate_message_annotations(MessageAnnotations),
    #'v1_0.modified'{delivery_failed = DeliveryFailed,
                     undeliverable_here = UndeliverableHere,
                     message_annotations = MA};
translate_delivery_state(released) -> #'v1_0.released'{};
translate_delivery_state(received) -> #'v1_0.received'{}.

translate_role(receiver) -> true.

maybe_notify_link_credit(#link{role = sender,
                               link_credit = 0},
                         #link{role = sender,
                               link_credit = NewCredit} = NewLink)
  when NewCredit > 0 ->
    notify_link(NewLink, credited);
maybe_notify_link_credit(_Old, _New) ->
    ok.

notify_link_attached(Link, Perf, #state{connection_config = Cfg}) ->
    What = case Cfg of
               #{notify_with_performative := true} ->
                   {attached, Perf};
               _ ->
                   attached
           end,
    notify_link(Link, What).

notify_link_detached(Link,
                     Perf = #'v1_0.detach'{error = Err},
                     #state{connection_config = Cfg}) ->
    Reason = case Cfg of
                 #{notify_with_performative := true} ->
                     Perf;
                 _ ->
                     reason(Err)
             end,
    notify_link(Link, {detached, Reason}).

notify_link(#link{notify = Pid, ref = Ref}, What) ->
    Evt = {amqp10_event, {link, Ref, What}},
    Pid ! Evt,
    ok.

notify_session_begun(Perf, #state{notify = Pid,
                                  connection_config = Cfg}) ->
    Evt = case Cfg of
              #{notify_with_performative := true} ->
                  {begun, Perf};
              _ ->
                  begun
          end,
    Pid ! amqp10_session_event(Evt),
    ok.

notify_session_ended(Perf = #'v1_0.end'{error = Err},
                     #state{notify = Pid,
                            connection_config = Cfg}) ->
    Reason = case Cfg of
                 #{notify_with_performative := true} ->
                     Perf;
                 _ ->
                     reason(Err)
             end,
    Pid ! amqp10_session_event({ended, Reason}),
    ok.

notify_disposition(Pid, DeliveryStateDeliveryTag) ->
    Pid ! {amqp10_disposition, DeliveryStateDeliveryTag},
    ok.

book_transfer_send(Num, #link{output_handle = Handle} = Link,
                   #state{next_outgoing_id = NextOutgoingId,
                          outgoing_delivery_id = DeliveryId,
                          remote_incoming_window = RIW,
                          links = Links} = State) ->
    State#state{next_outgoing_id = add(NextOutgoingId, Num),
                outgoing_delivery_id = add(DeliveryId, 1),
                remote_incoming_window = RIW-Num,
                links = Links#{Handle => book_link_transfer_send(Link)}}.

book_partial_transfer_received(#state{next_incoming_id = NID,
                                      incoming_window = IW,
                                      remote_outgoing_window = ROW} = State0) ->
    State = State0#state{next_incoming_id = add(NID, 1),
                         incoming_window = IW - 1,
                         remote_outgoing_window = ROW - 1},
    maybe_widen_incoming_window(State).

book_transfer_received(State = #state{connection_config =
                                      #{transfer_limit_margin := Margin}},
                       #link{link_credit = Margin} = Link) ->
    {transfer_limit_exceeded, Link, State};
book_transfer_received(#state{next_incoming_id = NID,
                              incoming_window = IW,
                              remote_outgoing_window = ROW,
                              links = Links} = State0,
                       #link{output_handle = OutHandle,
                             delivery_count = DC,
                             link_credit = LC,
                             available = Avail} = Link) ->
    Link1 = Link#link{delivery_count = add(DC, 1),
                      link_credit = LC - 1,
                      %% "the receiver MUST maintain a floor of zero in its
                      %% calculation of the value of available" [2.6.7]
                      available = max(0, Avail - 1)},
    State1 = State0#state{links = Links#{OutHandle => Link1},
                          next_incoming_id = add(NID, 1),
                          incoming_window = IW - 1,
                          remote_outgoing_window = ROW - 1},
    State = maybe_widen_incoming_window(State1),
    case Link1 of
        #link{link_credit = 0,
              auto_flow = never} ->
            {credit_exhausted, Link1, State};
        _ ->
            {ok, Link1, State}
    end.

maybe_widen_incoming_window(
  State0 = #state{incoming_window = IncomingWindow,
                  auto_flow = {RenewWhenBelow, NewWindowSize}})
  when IncomingWindow < RenewWhenBelow ->
    State = State0#state{incoming_window = NewWindowSize},
    send_flow_session(State),
    State;
maybe_widen_incoming_window(State) ->
    State.

auto_flow(#link{link_credit = LC,
                auto_flow = {RenewWhenBelow, Credit},
                output_handle = OutHandle,
                incoming_unsettled = Unsettled},
          State)
  when LC + map_size(Unsettled) < RenewWhenBelow ->
    send_flow_link(OutHandle,
                   #'v1_0.flow'{link_credit = {uint, Credit}},
                   RenewWhenBelow, State);
auto_flow(_, State) ->
    State.

update_link(Link = #link{output_handle = OutHandle},
            State = #state{links = Links}) ->
            State#state{links = Links#{OutHandle => Link}}.

book_link_transfer_send(Link = #link{link_credit = LC,
                                     delivery_count = DC}) ->
    Link#link{link_credit = LC - 1,
              delivery_count = add(DC, 1)}.

append_partial_transfer(Transfer, Payload,
                        #link{partial_transfers = undefined} = Link) ->
    Link#link{partial_transfers = {Transfer, [Payload]}};
append_partial_transfer(_Transfer, Payload,
                        #link{partial_transfers = {T, Payloads}} = Link) ->
    Link#link{partial_transfers = {T, [Payload | Payloads]}}.

complete_partial_transfer(Transfer, Payload,
                          #link{partial_transfers = undefined} = Link) ->
    {Transfer, Payload, Link};
complete_partial_transfer(_Transfer, Payload,
                          #link{partial_transfers = {T, Payloads}} = Link) ->
    {T, iolist_to_binary(lists:reverse([Payload | Payloads])),
     Link#link{partial_transfers = undefined}}.

decode_as_msg(Transfer, Payload, undefined) ->
    Sections = amqp10_framing:decode_bin(Payload),
    {ok, amqp10_msg:from_amqp_records([Transfer | Sections])};
decode_as_msg(Transfer, Payload, FooterOpt) ->
    PosSections = decode_sections([], Payload, byte_size(Payload), 0),
    Sections = lists:map(fun({_Pos, S}) -> S end, PosSections),
    Msg = amqp10_msg:from_amqp_records([Transfer | Sections]),
    OkMsg = {ok, Msg},
    case lists:last(PosSections) of
        {PosFooter, #'v1_0.footer'{content = Content}}
          when is_list(Content) ->
            Key = case FooterOpt of
                      crc32 -> <<"x-opt-crc-32">>;
                      adler32 -> <<"x-opt-adler-32">>
                  end,
            case lists:search(fun({{symbol, K}, {uint, _Checksum}})
                                    when K =:= Key ->
                                      true;
                                 (_) ->
                                      false
                              end, Content) of
                {value, {{symbol, Key}, {uint, Expected}}} ->
                    {value, {PosBare, _}} = lists:search(fun({_Pos, S}) ->
                                                                 is_bare_message_section(S)
                                                         end, PosSections),
                    BareBin = binary_part(Payload, PosBare, PosFooter - PosBare),
                    case erlang:FooterOpt(BareBin) of
                        Expected -> OkMsg;
                        Actual -> {checksum_error, Expected, Actual}
                    end;
                false ->
                    OkMsg
            end;
        _ ->
            OkMsg
    end.

decode_sections(PosSections, _Payload, PayloadSize, PayloadSize) ->
    lists:reverse(PosSections);
decode_sections(PosSections, Payload, PayloadSize, Parsed)
  when PayloadSize > Parsed ->
    Bin = binary_part(Payload, Parsed, PayloadSize - Parsed),
    {Described, NumBytes} = amqp10_binary_parser:parse(Bin),
    Section = amqp10_framing:decode(Described),
    decode_sections([{Parsed, Section} | PosSections],
                    Payload,
                    PayloadSize,
                    Parsed + NumBytes).

amqp10_session_event(Evt) ->
    {amqp10_event, {session, self(), Evt}}.

socket_send(Sock, Data) ->
    case amqp10_client_socket:send(Sock, Data) of
        ok ->
            ok;
        {error, _Reason} ->
            throw({stop, normal})
    end.

%% Only notify of credit exhaustion when not using auto flow.
notify_credit_exhausted(Link = #link{auto_flow = never}) ->
    ok = notify_link(Link, credit_exhausted);
notify_credit_exhausted(_Link) ->
    ok.

-spec make_link_ref(link_role(), pid(), output_handle()) ->
    link_ref().
make_link_ref(Role, Session, Handle) ->
    #link_ref{role = Role, session = Session, link_handle = Handle}.

translate_message_annotations(MA)
  when map_size(MA) > 0 ->
    {map, maps:fold(fun(K, V, Acc) ->
                            [{sym(K), amqp10_client_types:infer(V)} | Acc]
                    end, [], MA)};
translate_message_annotations(_MA) ->
    undefined.

sym(B) when is_binary(B) -> {symbol, B};
sym(B) when is_list(B) -> {symbol, list_to_binary(B)};
sym(B) when is_atom(B) -> {symbol, atom_to_binary(B, utf8)}.

reason(undefined) -> normal;
reason(Other) -> Other.

role_to_boolean(sender) ->
    ?AMQP_ROLE_SENDER;
role_to_boolean(receiver) ->
    ?AMQP_ROLE_RECEIVER.

boolean_to_role(?AMQP_ROLE_SENDER) ->
    sender;
boolean_to_role(?AMQP_ROLE_RECEIVER) ->
    receiver.

default(undefined, Default) -> Default;
default(Thing, _Default) -> Thing.

format_status(Status = #{data := Data0}) ->
    #state{channel = Channel,
           remote_channel = RemoteChannel,
           next_incoming_id = NextIncomingId,
           incoming_window = IncomingWindow,
           auto_flow = SessionAutoFlow,
           next_outgoing_id = NextOutgoingId,
           remote_incoming_window = RemoteIncomingWindow,
           remote_outgoing_window = RemoteOutgoingWindow,
           reader = Reader,
           socket = Socket,
           links = Links0,
           link_index = LinkIndex,
           link_handle_index = LinkHandleIndex,
           next_link_handle = NextLinkHandle,
           early_attach_requests = EarlyAttachRequests,
           connection_config = ConnectionConfig,
           outgoing_delivery_id = OutgoingDeliveryId,
           outgoing_unsettled = OutgoingUnsettled,
           notify = Notify
          } = Data0,
    Links = maps:map(
              fun(_OutputHandle,
                  #link{name = Name,
                        ref = Ref,
                        state = State,
                        notify = LinkNotify,
                        output_handle = OutputHandle,
                        input_handle = InputHandle,
                        role = Role,
                        target = Target,
                        max_message_size = MaxMessageSize,
                        delivery_count = DeliveryCount,
                        link_credit = LinkCredit,
                        available = Available,
                        drain = Drain,
                        partial_transfers = PartialTransfers0,
                        auto_flow = AutoFlow,
                        incoming_unsettled = IncomingUnsettled,
                        footer_opt = FooterOpt
                       }) ->
                      PartialTransfers = case PartialTransfers0 of
                                             undefined ->
                                                 0;
                                             {#'v1_0.transfer'{}, Binaries} ->
                                                 length(Binaries)
                                         end,
                      #{name => Name,
                        ref => Ref,
                        state => State,
                        notify => LinkNotify,
                        output_handle => OutputHandle,
                        input_handle => InputHandle,
                        role => Role,
                        target => Target,
                        max_message_size => MaxMessageSize,
                        delivery_count => DeliveryCount,
                        link_credit => LinkCredit,
                        available => Available,
                        drain => Drain,
                        partial_transfers => PartialTransfers,
                        auto_flow => AutoFlow,
                        incoming_unsettled => maps:size(IncomingUnsettled),
                        footer_opt => FooterOpt
                       }
              end, Links0),
    Data = #{channel => Channel,
             remote_channel => RemoteChannel,
             next_incoming_id => NextIncomingId,
             incoming_window => IncomingWindow,
             auto_flow => SessionAutoFlow,
             next_outgoing_id => NextOutgoingId,
             remote_incoming_window => RemoteIncomingWindow,
             remote_outgoing_window => RemoteOutgoingWindow,
             reader => Reader,
             socket => Socket,
             links => Links,
             link_index => LinkIndex,
             link_handle_index => LinkHandleIndex,
             next_link_handle => NextLinkHandle,
             early_attach_requests => length(EarlyAttachRequests),
             connection_config => maps:remove(sasl, ConnectionConfig),
             outgoing_delivery_id => OutgoingDeliveryId,
             outgoing_unsettled => maps:size(OutgoingUnsettled),
             notify => Notify},
    Status#{data := Data}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

handle_session_flow_test() ->
    % see spec section: 2.5.6 for logic
    Flow = #'v1_0.flow'{next_incoming_id = {uint, 50},
                        next_outgoing_id = {uint, 42},
                        incoming_window = {uint, 1000},
                        outgoing_window = {uint, 2000}},
    State0 = #state{next_outgoing_id = 51},
    State = handle_session_flow(Flow, State0),
    42 = State#state.next_incoming_id,
    2000 = State#state.remote_outgoing_window,
    50 + 1000 - 51 = State#state.remote_incoming_window.

handle_session_flow_pre_begin_test() ->
    % see spec section: 2.5.6 for logic
    Flow = #'v1_0.flow'{next_incoming_id = undefined,
                        next_outgoing_id = {uint, 42},
                        incoming_window = {uint, 1000},
                        outgoing_window = {uint, 2000}},
    State0 = #state{next_outgoing_id = 51},
    State = handle_session_flow(Flow, State0),
    42 = State#state.next_incoming_id,
    2000 = State#state.remote_outgoing_window,
    % using serial number arithmetic:
    % ?INITIAL_OUTGOING_TRANSFER_ID + 1000 = 998
    ?assertEqual(998 - 51, State#state.remote_incoming_window).

handle_link_flow_sender_test() ->
    DeliveryCountRcv = 55,
    DeliveryCountSnd = DeliveryCountRcv + 2,
    LinkCreditRcv = 42,
    Link = #link{role = sender,
                 output_handle = 99,
                 link_credit = 0,
                 delivery_count = DeliveryCountSnd},
    Flow = #'v1_0.flow'{handle = {uint, 45},
                        link_credit = {uint, LinkCreditRcv},
                        delivery_count = {uint, DeliveryCountRcv}
                       },
    {ok, Outcome} = handle_link_flow(Flow, Link),
    % see section 2.6.7
    ?assertEqual(DeliveryCountRcv + LinkCreditRcv - DeliveryCountSnd,
                 Outcome#link.link_credit),

    % receiver does not yet know the delivery_count
    {ok, Outcome2} = handle_link_flow(Flow#'v1_0.flow'{delivery_count = undefined},
                                      Link),
    % using serial number arithmetic:
    % ?INITIAL_DELIVERY_COUNT + LinkCreditRcv - DeliveryCountSnd = -18
    % but we maintain a floor of zero
    ?assertEqual(0, Outcome2#link.link_credit).

handle_link_flow_sender_drain_test() ->
    Handle = 45,
    SndDeliveryCount = 55,
    RcvLinkCredit = 42,
    Link = #link{role = sender, output_handle = 99,
                 available = 0, link_credit = 20,
                 delivery_count = SndDeliveryCount},
    Flow = #'v1_0.flow'{handle = {uint, Handle},
                        link_credit = {uint, RcvLinkCredit},
                        drain = true},
    {send_flow, Outcome} = handle_link_flow(Flow, Link),
    0 = Outcome#link.link_credit,
    ExpectedDC = SndDeliveryCount + RcvLinkCredit,
    ExpectedDC = Outcome#link.delivery_count.


handle_link_flow_receiver_test() ->
    Handle = 45,
    DeliveryCount = 55,
    SenderDC = 57,
    Link = #link{role = receiver, output_handle = 99,
                 link_credit = 0, delivery_count = DeliveryCount},
    Flow = #'v1_0.flow'{handle = {uint, Handle},
                        delivery_count = {uint, SenderDC},
                        available = 99,
                        drain = true, % what to do?
                        link_credit = {uint, 0}
                       },
    {ok, Outcome} = handle_link_flow(Flow, Link),
    % see section 2.6.7
    99 = Outcome#link.available,
    true = Outcome#link.drain,
    SenderDC = Outcome#link.delivery_count. % maintain delivery count

translate_filters_empty_map_test() ->
    undefined = translate_filters(#{}).

translate_filters_legacy_amqp_direct_binding_filter_test() ->
    {map,
        [{
            {symbol,<<"apache.org:legacy-amqp-direct-binding:string">>},
            {described, {symbol, <<"apache.org:legacy-amqp-direct-binding:string">>}, {utf8,<<"my topic">>}}
        }]
    } = translate_filters(#{<<"apache.org:legacy-amqp-direct-binding:string">> => <<"my topic">>}).

translate_filters_legacy_amqp_topic_binding_filter_test() ->
    {map,
        [{
            {symbol, <<"apache.org:legacy-amqp-topic-binding:string">>},
            {described, {symbol, <<"apache.org:legacy-amqp-topic-binding:string">>}, {utf8, <<"*.stock.#">>}}
        }]
    } = translate_filters(#{<<"apache.org:legacy-amqp-topic-binding:string">> => <<"*.stock.#">>}).

translate_filters_legacy_amqp_headers_binding_filter_test() ->
    {map,
        [{
            {symbol, <<"apache.org:legacy-amqp-headers-binding:map">>},
            {described, {symbol, <<"apache.org:legacy-amqp-headers-binding:map">>},
             {map, [
                    {{utf8, <<"x-match">>}, {utf8, <<"all">>}},
                    {{utf8, <<"foo">>}, {utf8, <<"bar">>}},
                    {{utf8, <<"bar">>}, {utf8, <<"baz">>}}
                ]
            }}
        }]
    } = translate_filters(#{<<"apache.org:legacy-amqp-headers-binding:map">> => #{
            <<"x-match">> => <<"all">>,
            <<"x-something">> => <<"ignored">>,
            <<"bar">> => <<"baz">>,
            <<"foo">> => <<"bar">>
        }}).

translate_filters_legacy_amqp_no_local_filter_test() ->
    {map,
        [{
            {symbol, <<"apache.org:no-local-filter:list">>},
            {described, {symbol, <<"apache.org:no-local-filter:list">>},
             {list, [{utf8, <<"foo">>}, {utf8, <<"bar">>}]}}
        }]
    } = translate_filters(#{<<"apache.org:no-local-filter:list">> => [<<"foo">>, <<"bar">>]}).

translate_filters_selector_filter_test() ->
    {map,
        [{
            {symbol, <<"apache.org:selector-filter:string">>},
            {described, {symbol, <<"apache.org:selector-filter:string">>},
             {utf8, <<"amqp.annotation.x-opt-enqueuedtimeutc > 123456789">>}}
        }]
    } = translate_filters(#{<<"apache.org:selector-filter:string">> => <<"amqp.annotation.x-opt-enqueuedtimeutc > 123456789">>}).

translate_filters_multiple_filters_test() ->
    {map, Actual} = translate_filters(
                      #{
                        <<"apache.org:legacy-amqp-direct-binding:string">> => <<"my topic">>,
                        <<"apache.org:selector-filter:string">> => <<"amqp.annotation.x-opt-enqueuedtimeutc > 123456789">>
                       }),
    Expected = [{{symbol, <<"apache.org:selector-filter:string">>},
                 {described, {symbol, <<"apache.org:selector-filter:string">>},
                  {utf8, <<"amqp.annotation.x-opt-enqueuedtimeutc > 123456789">>}}},
                {{symbol, <<"apache.org:legacy-amqp-direct-binding:string">>},
                 {described, {symbol, <<"apache.org:legacy-amqp-direct-binding:string">>}, {utf8,<<"my topic">>}}}],
    ActualSorted = lists:sort(Actual),
    ExpectedSorted = lists:sort(Expected),
    ExpectedSorted = ActualSorted.
-endif.
