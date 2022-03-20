%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(amqp10_client_session).

-behaviour(gen_statem).

-include("amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

%% Public API.
-export(['begin'/1,
         begin_sync/1,
         begin_sync/2,
         'end'/1,
         attach/2,
         detach/2,
         transfer/3,
         flow/4,
         disposition/6
        ]).

%% Private API.
-export([start_link/4,
         socket_ready/2
        ]).

%% gen_statem callbacks
-export([
         init/1,
         terminate/3,
         code_change/4,
         callback_mode/0
        ]).

%% gen_statem state callbacks.
-export([
         unmapped/3,
         begin_sent/3,
         mapped/3,
         end_sent/3
        ]).

-define(MAX_SESSION_WINDOW_SIZE, 65535).
-define(DEFAULT_MAX_HANDLE, 16#ffffffff).
-define(DEFAULT_TIMEOUT, 5000).
-define(INITIAL_OUTGOING_ID, 0).
-define(INITIAL_DELIVERY_COUNT, 0).

% -type from() :: {pid(), term()}.

-type transfer_id() :: non_neg_integer().
-type link_handle() :: non_neg_integer().
-type link_name() :: binary().
-type link_address() :: binary().
-type link_role() :: sender | receiver.
-type link_source() :: link_address() | undefined.
-type link_target() :: {pid, pid()} | binary() | undefined.

-type snd_settle_mode() :: unsettled | settled | mixed.
-type rcv_settle_mode() :: first | second.

-type terminus_durability() :: none | configuration | unsettled_state.

-type target_def() :: #{address => link_address(),
                        durable => terminus_durability()}.
-type source_def() :: #{address => link_address(),
                        durable => terminus_durability()}.

-type attach_role() :: {sender, target_def()} | {receiver, source_def(), pid()}.

% http://www.amqp.org/specification/1.0/filters
-type filter() :: #{binary() => binary() | map() | list(binary())}.
-type properties() :: #{binary() => tuple()}.

-type attach_args() :: #{name => binary(),
                         role => attach_role(),
                         snd_settle_mode => snd_settle_mode(),
                         rcv_settle_mode => rcv_settle_mode(),
                         filter => filter(),
                         properties => properties()
                        }.

-type link_ref() :: #link_ref{}.

-export_type([snd_settle_mode/0,
              rcv_settle_mode/0,
              terminus_durability/0,
              attach_args/0,
              attach_role/0,
              target_def/0,
              source_def/0,
              properties/0,
              filter/0]).

-record(link,
        {name :: link_name(),
         ref :: link_ref(),
         state = detached :: detached | attach_sent | attached | detach_sent,
         notify :: pid(),
         output_handle :: link_handle(),
         input_handle :: link_handle() | undefined,
         role :: link_role(),
         source :: link_source(),
         target :: link_target(),
         delivery_count = 0 :: non_neg_integer(),
         link_credit = 0 :: non_neg_integer(),
         link_credit_unsettled = 0 :: non_neg_integer(),
         available = 0 :: non_neg_integer(),
         drain = false :: boolean(),
         partial_transfers :: undefined | {#'v1_0.transfer'{}, [binary()]},
         auto_flow :: never | {auto, non_neg_integer(), non_neg_integer()}
         }).

-record(state,
        {channel :: pos_integer(),
         remote_channel :: pos_integer() | undefined,
         next_incoming_id = 0 :: transfer_id(),
         incoming_window = ?MAX_SESSION_WINDOW_SIZE :: non_neg_integer(),
         next_outgoing_id = ?INITIAL_OUTGOING_ID + 1 :: transfer_id(),
         outgoing_window = ?MAX_SESSION_WINDOW_SIZE  :: non_neg_integer(),
         remote_incoming_window = 0 :: non_neg_integer(),
         remote_outgoing_window = 0 :: non_neg_integer(),
         reader :: pid(),
         socket :: amqp10_client_connection:amqp10_socket() | undefined,
         links = #{} :: #{link_handle() => #link{}},
         % maps link name to outgoing link handle
         link_index = #{} :: #{link_name() => link_handle()},
         % maps incoming handle to outgoing
         link_handle_index = #{} :: #{link_handle() => link_handle()},
         next_link_handle = 0 :: link_handle(),
         early_attach_requests = [] :: [term()],
         connection_config = #{} :: amqp10_client_connection:connection_config(),
         % the unsettled map needs to go in the session state as a disposition
         % can reference transfers for many different links
         unsettled = #{} :: #{transfer_id() => {amqp10_msg:delivery_tag(),
                                                any()}}, %TODO: refine as FsmRef
         incoming_unsettled = #{} :: #{transfer_id() => link_handle()},
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
    begin_sync(Connection, ?DEFAULT_TIMEOUT).

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
    gen_statem:call(Session, {attach, Args}, {dirty_timeout, ?TIMEOUT}).

-spec detach(pid(), link_handle()) -> ok | {error, link_not_found | half_attached}.
detach(Session, Handle) ->
    gen_statem:call(Session, {detach, Handle}, {dirty_timeout, ?TIMEOUT}).

-spec transfer(pid(), amqp10_msg:amqp10_msg(), timeout()) ->
    ok | {error, insufficient_credit | link_not_found | half_attached}.
transfer(Session, Amqp10Msg, Timeout) ->
    [Transfer | Records] = amqp10_msg:to_amqp_records(Amqp10Msg),
    gen_statem:call(Session, {transfer, Transfer, Records},
                    {dirty_timeout, Timeout}).

flow(Session, Handle, Flow, RenewAfter) ->
    gen_statem:cast(Session, {flow, Handle, Flow, RenewAfter}).

-spec disposition(pid(), link_role(), transfer_id(), transfer_id(), boolean(),
                  amqp10_client_types:delivery_state()) -> ok.
disposition(Session, Role, First, Last, Settled, DeliveryState) ->
    gen_statem:call(Session, {disposition, Role, First, Last, Settled,
                              DeliveryState}, {dirty_timeout, ?TIMEOUT}).



%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

start_link(From, Channel, Reader, ConnConfig) ->
    gen_statem:start_link(?MODULE, [From, Channel, Reader, ConnConfig], []).

-spec socket_ready(pid(), amqp10_client_connection:amqp10_socket()) -> ok.
socket_ready(Pid, Socket) ->
    gen_statem:cast(Pid, {socket_ready, Socket}).

%% -------------------------------------------------------------------
%% gen_statem callbacks.
%% -------------------------------------------------------------------

callback_mode() -> [state_functions].

init([FromPid, Channel, Reader, ConnConfig]) ->
    process_flag(trap_exit, true),
    amqp10_client_frame_reader:register_session(Reader, self(), Channel),
    State = #state{notify = FromPid, channel = Channel, reader = Reader,
                   connection_config = ConnConfig},
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
                               outgoing_window = {uint, OutWindow}},
           #state{early_attach_requests = EARs} = State) ->

    State1 = State#state{remote_channel = RemoteChannel},
    State2 = lists:foldr(fun({From, Attach}, S) ->
                                 {S2, H} = send_attach(fun send/2, Attach, From, S),
                                 gen_statem:reply(From, {ok, H}),
                                 S2
                         end, State1, EARs),

    ok = notify_session_begun(State2),

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
mapped(cast, {flow, OutHandle, Flow0, RenewAfter}, State0) ->
    State = send_flow(fun send/2, OutHandle, Flow0, RenewAfter, State0),
    {next_state, mapped, State};
mapped(cast, #'v1_0.end'{error = Err}, State) ->
    %% We receive the first end frame, reply and terminate.
    _ = send_end(State),
    % TODO: send notifications for links?
    Reason = case Err of
                 undefined -> normal;
                 _ -> Err
             end,
    ok = notify_session_ended(State, Reason),
    {stop, normal, State};
mapped(cast, #'v1_0.attach'{name = {utf8, Name},
                            initial_delivery_count = IDC,
                            handle = {uint, InHandle}},
       #state{links = Links, link_index = LinkIndex,
              link_handle_index = LHI} = State0) ->

    #{Name := OutHandle} = LinkIndex,
    #{OutHandle := Link0} = Links,
    ok = notify_link_attached(Link0),

    DeliveryCount = case Link0 of
                        #link{role = sender, delivery_count = DC} -> DC;
                        _ -> unpack(IDC)
                    end,
    Link = Link0#link{input_handle = InHandle, state = attached,
                      delivery_count = DeliveryCount},
    State = State0#state{links = Links#{OutHandle => Link},
                         link_index = maps:remove(Name, LinkIndex),
                         link_handle_index = LHI#{InHandle => OutHandle}},
    {next_state, mapped, State};
mapped(cast, #'v1_0.detach'{handle = {uint, InHandle},
                            error = Err},
        #state{links = Links, link_handle_index = LHI} = State0) ->
    with_link(InHandle, State0,
              fun (#link{output_handle = OutHandle} = Link, State) ->
                      Reason = case Err of
                                   undefined -> normal;
                                   Err -> Err
                               end,
                      ok = notify_link_detached(Link, Reason),
                      {next_state, mapped,
                       State#state{links = maps:remove(OutHandle, Links),
                                   link_handle_index = maps:remove(InHandle, LHI)}}
              end);
mapped(cast, #'v1_0.flow'{handle = undefined} = Flow, State0) ->
    State = handle_session_flow(Flow, State0),
    {next_state, mapped, State};
mapped(cast, #'v1_0.flow'{handle = {uint, InHandle}} = Flow,
       #state{links = Links} = State0) ->

    State = handle_session_flow(Flow, State0),

    {ok, #link{output_handle = OutHandle} = Link0} =
        find_link_by_input_handle(InHandle, State),

     % TODO: handle `send_flow` return tag
    {ok, Link} = handle_link_flow(Flow, Link0),
    ok = maybe_notify_link_credit(Link0, Link),
    Links1 = Links#{OutHandle => Link},
    State1 = State#state{links = Links1},
    {next_state, mapped, State1};
mapped(cast, {#'v1_0.transfer'{handle = {uint, InHandle},
                         more = true} = Transfer, Payload},
                         #state{links = Links} = State0) ->

    {ok, #link{output_handle = OutHandle} = Link} =
        find_link_by_input_handle(InHandle, State0),

    Link1 = append_partial_transfer(Transfer, Payload, Link),

    State = book_partial_transfer_received(
              State0#state{links = Links#{OutHandle => Link1}}),
    {next_state, mapped, State};
mapped(cast, {#'v1_0.transfer'{handle = {uint, InHandle},
                         delivery_id = MaybeDeliveryId,
                         settled = Settled} = Transfer0, Payload0},
                         #state{incoming_unsettled = Unsettled0} = State0) ->

    {ok, #link{target = {pid, TargetPid},
               output_handle = OutHandle,
               ref = LinkRef} = Link0} =
        find_link_by_input_handle(InHandle, State0),

    {Transfer, Payload, Link} = complete_partial_transfer(Transfer0, Payload0, Link0),
    Msg = decode_as_msg(Transfer, Payload),

    % stash the DeliveryId - not sure for what yet
    Unsettled = case MaybeDeliveryId of
                    {uint, DeliveryId} when Settled =/= true ->
                        Unsettled0#{DeliveryId => OutHandle};
                    _ ->
                        Unsettled0
                end,

    % link bookkeeping
    % notify when credit is exhausted (link_credit = 0)
    % detach the Link with a transfer-limit-exceeded error code if further
    % transfers are received
    case book_transfer_received(Settled,
                                State0#state{incoming_unsettled = Unsettled},
                                Link) of
        {ok, State} ->
            % deliver
            TargetPid ! {amqp10_msg, LinkRef, Msg},
            State1 = auto_flow(Link, State),
            {next_state, mapped, State1};
        {credit_exhausted, State} ->
            TargetPid ! {amqp10_msg, LinkRef, Msg},
            ok = notify_link(Link, credit_exhausted),
            {next_state, mapped, State};
        {transfer_limit_exceeded, State} ->
            logger:warning("transfer_limit_exceeded for link ~p", [Link]),
            Link1 = detach_with_error_cond(Link, State,
                                           ?V_1_0_LINK_ERROR_TRANSFER_LIMIT_EXCEEDED),
            {next_state, mapped, update_link(Link1, State)}
    end;


% role=true indicates the disposition is from a `receiver`. i.e. from the
% clients point of view these are dispositions relating to `sender`links
mapped(cast, #'v1_0.disposition'{role = true, settled = true, first = {uint, First},
                           last = Last0, state = DeliveryState},
       #state{unsettled = Unsettled0} = State) ->
    Last = case Last0 of
               undefined -> First;
               {uint, L} -> L
           end,
    % TODO: no good if the range becomes very large!! refactor
    Unsettled =
        lists:foldl(fun(Id, Acc) ->
                            case Acc of
                                #{Id := {DeliveryTag, Receiver}} ->
                                    S = translate_delivery_state(DeliveryState),
                                    ok = notify_disposition(Receiver,
                                                            {S, DeliveryTag}),
                                    maps:remove(Id, Acc);
                                _ -> Acc
                            end
                    end, Unsettled0, lists:seq(First, Last)),

    {next_state, mapped, State#state{unsettled = Unsettled}};
mapped(cast, Frame, State) ->
    logger:warning("Unhandled session frame ~p in state ~p",
                             [Frame, State]),
    {next_state, mapped, State};
mapped({call, From},
       {transfer, #'v1_0.transfer'{handle = {uint, OutHandle},
                                   delivery_tag = {binary, DeliveryTag},
                                   settled = false} = Transfer0, Parts},
       #state{next_outgoing_id = NOI, links = Links,
              unsettled = Unsettled} = State) ->
    case Links of
        #{OutHandle := #link{input_handle = undefined}} ->
            {keep_state, State, [{reply, From, {error, half_attached}}]};
        #{OutHandle := #link{link_credit = LC}} when LC =< 0 ->
            {keep_state, State, [{reply, From, {error, insufficient_credit}}]};
        #{OutHandle := Link} ->
            Transfer = Transfer0#'v1_0.transfer'{delivery_id = uint(NOI),
                                                 resume = false},
            {ok, NumFrames} = send_transfer(Transfer, Parts, State),
            State1 = State#state{unsettled = Unsettled#{NOI => {DeliveryTag, From}}},
            {keep_state, book_transfer_send(NumFrames, Link, State1),
             [{reply, From, ok}]};
        _ ->
            {keep_state, State, [{reply, From, {error, link_not_found}}]}

    end;
mapped({call, From},
       {transfer, #'v1_0.transfer'{handle = {uint, OutHandle}} = Transfer0,
        Parts}, #state{next_outgoing_id = NOI,
                       links = Links} = State) ->
    case Links of
        #{OutHandle := #link{input_handle = undefined}} ->
            {keep_state_and_data, [{reply, From, {error, half_attached}}]};
        #{OutHandle := #link{link_credit = LC}} when LC =< 0 ->
            {keep_state_and_data, [{reply, From, {error, insufficient_credit}}]};
        #{OutHandle := Link} ->
            Transfer = Transfer0#'v1_0.transfer'{delivery_id = uint(NOI)},
            {ok, NumFrames} = send_transfer(Transfer, Parts, State),
            % TODO look into if erlang will correctly wrap integers during
            % binary conversion.
            {keep_state, book_transfer_send(NumFrames, Link, State),
             [{reply, From, ok}]};
        _ ->
            {keep_state, [{reply, From, {error, link_not_found}}]}
    end;

mapped({call, From},
       {disposition, Role, First, Last, Settled0, DeliveryState},
       #state{incoming_unsettled = Unsettled0,
              links = Links0} = State0) ->
    Disposition =
    begin
        DS = translate_delivery_state(DeliveryState),
        #'v1_0.disposition'{role = translate_role(Role),
                            first = {uint, First},
                            last = {uint, Last},
                            settled = Settled0,
                            state = DS}
    end,

    Ks = lists:seq(First, Last),
    Settled = maps:values(maps:with(Ks, Unsettled0)),
    Links = lists:foldl(fun (H, Acc) ->
                                #{H := #link{link_credit_unsettled = LCU} = L} = Acc,
                                Acc#{H => L#link{link_credit_unsettled = LCU-1}}
                        end, Links0, Settled),
    Unsettled = maps:without(Ks, Unsettled0),
    State = lists:foldl(fun(H, S) ->
                                #{H := L} = Links,
                                auto_flow(L, S)
                        end,
                        State0#state{incoming_unsettled = Unsettled,
                                     links = Links},
                        lists:usort(Settled)),

    Res = send(Disposition, State),

    {keep_state, State, [{reply, From, Res}]};

mapped({call, From}, {attach, Attach}, State) ->
    {State1, LinkRef} = send_attach(fun send/2, Attach, From, State),
    {keep_state, State1, [{reply, From, {ok, LinkRef}}]};

mapped({call, From}, Msg, State) ->
    {Reply, State1} = send_detach(fun send/2, Msg, From, State),
    {keep_state, State1, [{reply, From, Reply}]};

mapped(_EvtType, Msg, _State) ->
    logger:warning("amqp10_session: unhandled msg in mapped state ~W",
                          [Msg, 10]),
    keep_state_and_data.

end_sent(_EvtType, #'v1_0.end'{}, State) ->
    {stop, normal, State};
end_sent(_EvtType, _Frame, State) ->
    % just drop frames here
    {next_state, end_sent, State}.

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
                  incoming_window = InWin,
                  outgoing_window = OutWin} = State) ->
    Begin = #'v1_0.begin'{next_outgoing_id = uint(NextOutId),
                          incoming_window = uint(InWin),
                          outgoing_window = uint(OutWin) },
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

send_transfer(Transfer0, Parts0, #state{socket = Socket, channel = Channel,
                                        connection_config = Config}) ->
    OutMaxFrameSize = case Config of
                          #{outgoing_max_frame_size := undefined} ->
                              ?MAX_MAX_FRAME_SIZE;
                          #{outgoing_max_frame_size := Sz} -> Sz;
                          _ -> ?MAX_MAX_FRAME_SIZE
                      end,
    Transfer = amqp10_framing:encode_bin(Transfer0),
    TSize = iolist_size(Transfer),
    Parts = [amqp10_framing:encode_bin(P) || P <- Parts0],
    PartsBin = iolist_to_binary(Parts),

    % TODO: this does not take the extended header into account
    % see: 2.3
    MaxPayloadSize = OutMaxFrameSize - TSize - ?FRAME_HEADER_SIZE,

    Frames = build_frames(Channel, Transfer0, PartsBin, MaxPayloadSize, []),
    ok = socket_send(Socket, Frames),
    {ok, length(Frames)}.

send_flow(Send, OutHandle,
          #'v1_0.flow'{link_credit = {uint, Credit}} = Flow0, RenewAfter,
          #state{links = Links,
                 next_incoming_id = NII,
                 next_outgoing_id = NOI,
                 outgoing_window = OutWin,
                 incoming_window = InWin} = State) ->
    AutoFlow = case RenewAfter of
                   never -> never;
                   Limit -> {auto, Limit, Credit}
               end,
    #{OutHandle := #link{output_handle = H,
                         role = receiver,
                         delivery_count = DeliveryCount,
                         available = Available,
                         link_credit_unsettled = LCU} = Link} = Links,
    Flow = Flow0#'v1_0.flow'{handle = uint(H),
                             link_credit = uint(Credit),
                             next_incoming_id = uint(NII),
                             next_outgoing_id = uint(NOI),
                             outgoing_window = uint(OutWin),
                             incoming_window = uint(InWin),
                             delivery_count = uint(DeliveryCount),
                             available = uint(Available)},
    ok = Send(Flow, State),
    State#state{links = Links#{OutHandle =>
                               Link#link{link_credit = Credit,
                                         % need to add on the current LCU
                                         % to ensure we don't overcredit
                                         link_credit_unsettled = LCU + Credit,
                                         auto_flow = AutoFlow}}}.

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
make_source(#{role := {receiver, #{address := Address} = Target, _Pid}, filter := Filter}) ->
    Durable = translate_terminus_durability(maps:get(durable, Target, none)),
    TranslatedFilter = translate_filters(Filter),
    #'v1_0.source'{address = {utf8, Address},
                   durable = {uint, Durable},
                   filter = TranslatedFilter}.

make_target(#{role := {receiver, _Source, _Pid}}) ->
    #'v1_0.target'{};
make_target(#{role := {sender, #{address := Address} = Target}}) ->
    Durable = translate_terminus_durability(maps:get(durable, Target, none)),
    #'v1_0.target'{address = {utf8, Address},
                   durable = {uint, Durable}}.

make_properties(#{properties := Properties}) ->
    translate_properties(Properties);
make_properties(_) ->
    undefined.

translate_properties(Properties) when is_map(Properties) andalso map_size(Properties) =< 0 ->
    undefined;
translate_properties(Properties) when is_map(Properties) ->
    {map, maps:fold(fun translate_property/3, [], Properties)}.

translate_property(K, V, Acc) when is_tuple(V) ->
    [{{symbol, K}, V} | Acc].

translate_terminus_durability(none) -> 0;
translate_terminus_durability(configuration) -> 1;
translate_terminus_durability(unsettled_state) -> 2.

translate_filters(Filters) when is_map(Filters) andalso map_size(Filters) =< 0 -> undefined;
translate_filters(Filters) when is_map(Filters) -> {
    map,
    maps:fold(
        fun(<<"apache.org:legacy-amqp-direct-binding:string">> = K, V, Acc) when is_binary(V) ->
            [{{symbol, K}, {described, {symbol, K}, {utf8, V}}} | Acc];
        (<<"apache.org:legacy-amqp-topic-binding:string">> = K, V, Acc) when is_binary(V) ->
            [{{symbol, K}, {described, {symbol, K}, {utf8, V}}} | Acc];
        (<<"apache.org:legacy-amqp-headers-binding:map">> = K, V, Acc) when is_map(V) ->
            [{{symbol, K}, {described, {symbol, K}, translate_legacy_amqp_headers_binding(V)}} | Acc];
        (<<"apache.org:no-local-filter:list">> = K, V, Acc) when is_list(V) ->
            [{{symbol, K}, {described, {symbol, K}, lists:map(fun(Id) -> {utf8, Id} end, V)}} | Acc];
        (<<"apache.org:selector-filter:string">> = K, V, Acc) when is_binary(V) ->
                [{{symbol, K}, {described, {symbol, K}, {utf8, V}}} | Acc]
        end,
        [],
        Filters)
}.

% https://people.apache.org/~rgodfrey/amqp-1.0/apache-filters.html
translate_legacy_amqp_headers_binding(LegacyHeaders) -> {
    map,
    maps:fold(
        fun(<<"x-match">> = K, <<"any">> = V, Acc) ->
            [{{utf8, K}, {utf8, V}} | Acc];
        (<<"x-match">> = K, <<"all">> = V, Acc) ->
            [{{utf8, K}, {utf8, V}} | Acc];
        (<<"x-",_/binary>>, _, Acc) ->
            Acc;
        (K, V, Acc) ->
            [{{utf8, K}, {utf8, V}} | Acc]
        end,
        [],
        LegacyHeaders)
}.

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

detach_with_error_cond(Link = #link{output_handle = OutHandle}, State, Cond) ->
    Err = #'v1_0.error'{condition = Cond},
    Detach = #'v1_0.detach'{handle = uint(OutHandle),
                            closed = true,
                            error = Err},
    ok = send(Detach, State),
    Link#link{state = detach_sent}.

send_attach(Send, #{name := Name, role := Role} = Args, {FromPid, _},
      #state{next_link_handle = OutHandle, links = Links,
             link_index = LinkIndex} = State) ->

    Source = make_source(Args),
    Target = make_target(Args),
    Properties = make_properties(Args),

    {LinkTarget, RoleAsBool} = case Role of
                                   {receiver, _, Pid} ->
                                       {{pid, Pid}, true};
                                   {sender, #{address := TargetAddr}} ->
                                       {TargetAddr, false}
                               end,

    % create attach performative
    Attach = #'v1_0.attach'{name = {utf8, Name},
                            role = RoleAsBool,
                            handle = {uint, OutHandle},
                            source = Source,
                            properties = Properties,
                            initial_delivery_count =
                                {uint, ?INITIAL_DELIVERY_COUNT},
                            snd_settle_mode = snd_settle_mode(Args),
                            rcv_settle_mode = rcv_settle_mode(Args),
                            target = Target},
    ok = Send(Attach, State),

    Link = #link{name = Name,
                 ref = make_link_ref(element(1, Role), self(), OutHandle),
                 output_handle = OutHandle,
                 state = attach_sent,
                 role = element(1, Role),
                 notify = FromPid,
                 auto_flow = never,
                 target = LinkTarget},

    {State#state{links = Links#{OutHandle => Link},
                 next_link_handle = OutHandle + 1,
                 link_index = LinkIndex#{Name => OutHandle}}, Link#link.ref}.

-spec handle_session_flow(#'v1_0.flow'{}, #state{}) -> #state{}.
handle_session_flow(#'v1_0.flow'{next_incoming_id = MaybeNII,
                                 next_outgoing_id = {uint, NOI},
                                 incoming_window = {uint, InWin},
                                 outgoing_window = {uint, OutWin}},
       #state{next_outgoing_id = OurNOI} = State) ->
    NII = case MaybeNII of
              {uint, N} -> N;
              undefined -> ?INITIAL_OUTGOING_ID + 1
          end,
    State#state{next_incoming_id = NOI,
                remote_incoming_window = NII + InWin - OurNOI, % see: 2.5.6
                remote_outgoing_window = OutWin}.


-spec handle_link_flow(#'v1_0.flow'{}, #link{}) -> {ok | send_flow, #link{}}.
handle_link_flow(#'v1_0.flow'{drain = true, link_credit = {uint, TheirCredit}},
                 Link = #link{role = sender,
                              delivery_count = OurDC,
                              available = 0}) ->
    {send_flow, Link#link{link_credit = 0,
                          delivery_count = OurDC + TheirCredit}};
handle_link_flow(#'v1_0.flow'{delivery_count = MaybeTheirDC,
                              link_credit = {uint, TheirCredit}},
                 Link = #link{role = sender,
                              delivery_count = OurDC}) ->
    TheirDC = case MaybeTheirDC of
                  undefined -> ?INITIAL_DELIVERY_COUNT;
                  {uint, DC} -> DC
              end,
    LinkCredit = TheirDC + TheirCredit - OurDC,

    {ok, Link#link{link_credit = LinkCredit}};
handle_link_flow(#'v1_0.flow'{delivery_count = TheirDC,
                              available = Available,
                              drain = Drain},
                 Link = #link{role = receiver}) ->

    {ok, Link#link{delivery_count = unpack(TheirDC),
                   available = unpack(Available),
                   drain = Drain}}.

-spec find_link_by_input_handle(link_handle(), #state{}) ->
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
translate_delivery_state(#'v1_0.rejected'{}) -> rejected;
translate_delivery_state(#'v1_0.modified'{}) -> modified;
translate_delivery_state(#'v1_0.released'{}) -> released;
translate_delivery_state(#'v1_0.received'{}) -> received;
translate_delivery_state(accepted) -> #'v1_0.accepted'{};
translate_delivery_state(rejected) -> #'v1_0.rejected'{};
translate_delivery_state(modified) -> #'v1_0.modified'{};
translate_delivery_state(released) -> #'v1_0.released'{};
translate_delivery_state(received) -> #'v1_0.received'{}.

translate_role(sender) -> false;
translate_role(receiver) -> true.

maybe_notify_link_credit(#link{link_credit = 0, role = sender},
                         #link{link_credit = Credit} = Link)
  when Credit > 0 ->
    notify_link(Link, credited);
maybe_notify_link_credit(_Old, _New) ->
    ok.

notify_link_attached(Link) ->
    notify_link(Link, attached).

notify_link_detached(Link, Reason) ->
    notify_link(Link, {detached, Reason}).

notify_link(#link{notify = Pid, ref = Ref}, What) ->
    Evt = {amqp10_event, {link, Ref, What}},
    Pid ! Evt,
    ok.

notify_session_begun(#state{notify = Pid}) ->
    Pid ! amqp10_session_event(begun),
    ok.

notify_session_ended(#state{notify = Pid}, Reason) ->
    Pid ! amqp10_session_event({ended, Reason}),
    ok.

notify_disposition({Pid, _}, SessionDeliveryTag) ->
    Pid ! {amqp10_disposition, SessionDeliveryTag},
    ok.

book_transfer_send(Num, #link{output_handle = Handle} = Link,
                   #state{next_outgoing_id = NOI,
                          remote_incoming_window = RIW,
                          links = Links} = State) ->
    State#state{next_outgoing_id = NOI+Num,
                remote_incoming_window = RIW-Num,
                links = Links#{Handle => incr_link_counters(Link)}}.

book_partial_transfer_received(#state{next_incoming_id = NID,
                                      remote_outgoing_window = ROW} = State) ->
    State#state{next_incoming_id = NID+1,
                remote_outgoing_window = ROW-1}.

book_transfer_received(_Settled,
                       State = #state{connection_config =
                                      #{transfer_limit_margin := Margin}},
                       #link{link_credit = Margin}) ->
    {transfer_limit_exceeded, State};
book_transfer_received(Settled,
                       #state{next_incoming_id = NID,
                              remote_outgoing_window = ROW,
                              links = Links} = State,
                       #link{output_handle = OutHandle,
                             delivery_count = DC,
                             link_credit = LC,
                             link_credit_unsettled = LCU0} = Link) ->
    LCU = case Settled of
              true -> LCU0-1;
              _ -> LCU0
          end,

    Link1 = Link#link{delivery_count = DC+1,
                      link_credit = LC-1,
                      link_credit_unsettled = LCU},
    State1 = State#state{links = Links#{OutHandle => Link1},
                         next_incoming_id = NID+1,
                         remote_outgoing_window = ROW-1},
    case Link1 of
        #link{link_credit = 0,
              % only notify of credit exhaustion when
              % not using auto flow.
              auto_flow = never} ->
            {credit_exhausted, State1};
        _ -> {ok, State1}
    end.

auto_flow(#link{link_credit_unsettled = LCU,
                auto_flow = {auto, Limit, Credit},
                output_handle = OutHandle}, State)
  when LCU =< Limit ->
    send_flow(fun send/2, OutHandle,
              #'v1_0.flow'{link_credit = {uint, Credit}},
              Limit, State);
auto_flow(_Link, State) ->
    State.

update_link(Link = #link{output_handle = OutHandle},
            State = #state{links = Links}) ->
            State#state{links = Links#{OutHandle => Link}}.

incr_link_counters(#link{link_credit = LC, delivery_count = DC} = Link) ->
    Link#link{delivery_count = DC+1, link_credit = LC+1}.

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

decode_as_msg(Transfer, Payload) ->
    Records = amqp10_framing:decode_bin(Payload),
    amqp10_msg:from_amqp_records([Transfer | Records]).

amqp10_session_event(Evt) ->
    {amqp10_event, {session, self(), Evt}}.

socket_send(Sock, Data) ->
    case socket_send0(Sock, Data) of
        ok -> ok;
        {error, _Reason} ->
            throw({stop, normal})
    end.

-dialyzer({no_fail_call, socket_send0/2}).
socket_send0({tcp, Socket}, Data) ->
    gen_tcp:send(Socket, Data);
socket_send0({ssl, Socket}, Data) ->
    ssl:send(Socket, Data).

-spec make_link_ref(_, _, _) -> link_ref().
make_link_ref(Role, Session, Handle) ->
    #link_ref{role = Role, session = Session, link_handle = Handle}.

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
    ?INITIAL_OUTGOING_ID + 1 + 1000 - 51 = State#state.remote_incoming_window.

handle_link_flow_sender_test() ->
    Handle = 45,
    DeliveryCount = 55,
    Link = #link{role = sender, output_handle = 99,
                 link_credit = 0, delivery_count = DeliveryCount + 2},
    Flow = #'v1_0.flow'{handle = {uint, Handle},
                        link_credit = {uint, 42},
                        delivery_count = {uint, DeliveryCount}
                       },
    {ok, Outcome} = handle_link_flow(Flow, Link),
    % see section 2.6.7
    Expected = DeliveryCount + 42 - (DeliveryCount + 2),
    Expected = Outcome#link.link_credit,

    % receiver does not yet know the delivery_count
    {ok, Outcome2} = handle_link_flow(Flow#'v1_0.flow'{delivery_count = undefined},
                                Link),
    Expected2 = ?INITIAL_DELIVERY_COUNT + 42 - (DeliveryCount + 2),
    Expected2 = Outcome2#link.link_credit.

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
                        drain = true % what to do?
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
            {described, {symbol, <<"apache.org:no-local-filter:list">>}, [{utf8, <<"foo">>}, {utf8, <<"bar">>}]}
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
    {map,
        [
            {{symbol, <<"apache.org:selector-filter:string">>},
             {described, {symbol, <<"apache.org:selector-filter:string">>},
              {utf8, <<"amqp.annotation.x-opt-enqueuedtimeutc > 123456789">>}}},
            {{symbol, <<"apache.org:legacy-amqp-direct-binding:string">>},
             {described, {symbol, <<"apache.org:legacy-amqp-direct-binding:string">>}, {utf8,<<"my topic">>}}}
        ]
    } = translate_filters(#{
            <<"apache.org:legacy-amqp-direct-binding:string">> => <<"my topic">>,
            <<"apache.org:selector-filter:string">> => <<"amqp.annotation.x-opt-enqueuedtimeutc > 123456789">>
        }).
-endif.
