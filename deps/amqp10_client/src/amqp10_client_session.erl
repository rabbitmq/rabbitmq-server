-module(amqp10_client_session).

-behaviour(gen_fsm).

-include("amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

%% Public API.
-export(['begin'/1,
         begin_sync/1,
         begin_sync/2,
         'end'/1,
         attach/2,
         transfer/3,
         flow/3,
         disposition/5
        ]).

%% Private API.
-export([start_link/5,
         socket_ready/2
        ]).

%% gen_fsm callbacks.
-export([init/1,
         unmapped/2,
         unmapped/3,
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
-define(DEFAULT_TIMEOUT, 5000).
-define(INITIAL_OUTGOING_ID, 65535).
-define(INITIAL_DELIVERY_COUNT, 0).

-type transfer_id() :: non_neg_integer().
-type link_handle() :: non_neg_integer().
-type link_name() :: binary().
-type link_address() :: binary().
-type link_role() :: sender | receiver.
-type link_source() :: link_address() | undefined.
-type link_target() :: {pid, pid()} | binary() | undefined.

-type snd_settle_mode() :: unsettled | settled | mixed.
-type rcv_settle_mode() :: first | second.


-type target_def() :: #{address => link_address(), durable => boolean()}.
-type source_def() :: #{address => link_address()}.

-type attach_role() :: {sender, target_def()} | {receiver, source_def(), pid()}.

-type attach_args() :: #{name => binary(),
                         role => attach_role(),
                         snd_settle_mode => snd_settle_mode(),
                         rcv_settle_mode => rcv_settle_mode()
                        }.

-export_type([snd_settle_mode/0,
              rcv_settle_mode/0]).

-record(link,
        {name :: link_name(),
         output_handle :: link_handle(),
         input_handle :: link_handle() | undefined,
         role :: link_role(),
         source :: link_source(),
         target :: link_target(),
         delivery_count = 0 :: non_neg_integer(),
         link_credit = 0 :: non_neg_integer(),
         available = 0 :: non_neg_integer(),
         drain = false :: boolean(),
         partial_transfers :: undefined | {#'v1_0.transfer'{}, [binary()]}
         }).

-record(state,
        {owner :: pid(),
         channel :: pos_integer(),
         remote_channel :: pos_integer() | undefined,
         next_incoming_id = 0 :: transfer_id(),
         incoming_window = ?MAX_SESSION_WINDOW_SIZE :: non_neg_integer(),
         next_outgoing_id = ?INITIAL_OUTGOING_ID + 1 :: transfer_id(),
         outgoing_window = ?MAX_SESSION_WINDOW_SIZE  :: non_neg_integer(),
         remote_incoming_window = 0 :: non_neg_integer(),
         remote_outgoing_window = 0 :: non_neg_integer(),
         reader :: pid(),
         socket :: gen_tcp:socket() | undefined,
         links = #{} :: #{link_handle() => #link{}},
         link_index = #{} :: #{link_name() => link_handle()}, % maps incoming handle to outgoing
         link_handle_index = #{} :: #{link_handle() => link_handle()}, % maps incoming handle to outgoing
         next_link_handle = 0 :: link_handle(),
         next_delivery_id = 0 :: non_neg_integer(),
         early_attach_requests = [] :: [term()],
         pending_attach_requests = #{} :: #{link_name() => {pid(), any()}},
         connection_config = #{} :: amqp10_client_connection:connection_config(),
         % the unsettled map needs to go in the session state as a disposition
         % can reference transfers for many different links
         unsettled = #{} :: #{transfer_id() => {link_handle(), any()}}, %TODO: refine as FsmRef
         incoming_unsettled = #{} :: #{transfer_id() => link_handle()},
         notify :: boolean()
        }).


%% -------------------------------------------------------------------
%% Public API.
%% -------------------------------------------------------------------

-spec 'begin'(pid()) -> supervisor:startchild_ret().
'begin'(Connection) ->
    %% The connection process is responsible for allocating a channel
    %% number and contact the sessions supervisor to start a new session
    %% process.
    amqp10_client_connection:begin_session(Connection, false).

-spec begin_sync(pid()) -> supervisor:startchild_ret().
begin_sync(Connection) ->
    begin_sync(Connection, ?DEFAULT_TIMEOUT).

-spec begin_sync(pid(), non_neg_integer()) ->
    supervisor:startchild_ret() | session_timeout.
begin_sync(Connection, Timeout) ->
    {ok, Session} = amqp10_client_connection:begin_session(Connection, true),
    receive
        {session_begin, Session} -> {ok, Session}
    after Timeout -> session_timeout
    end.

-spec 'end'(pid()) -> ok.
'end'(Pid) ->
    gen_fsm:send_event(Pid, 'end').

-spec attach(pid(), attach_args()) -> {ok, link_handle()}.
attach(Session, Args) ->
    gen_fsm:sync_send_event(Session, {attach, Args}).

-spec transfer(pid(), amqp10_msg:amqp10_msg(), timeout()) ->
    ok | insufficient_credit | amqp10_client_types:delivery_state().
transfer(Session, Amqp10Msg, Timeout) ->
    [Transfer | Records] = amqp10_msg:to_amqp_records(Amqp10Msg),
    gen_fsm:sync_send_event(Session, {transfer, Transfer, Records}, Timeout).

flow(Session, Handle, Flow) ->
    gen_fsm:send_event(Session, {flow, Handle, Flow}).

-spec disposition(pid(), transfer_id(), transfer_id(), boolean(),
               amqp10_client_types:delivery_state()) -> ok.
disposition(Session, First, Last, Settled, DeliveryState) ->
    gen_fsm:sync_send_event(Session, {disposition, First, Last, Settled,
                                      DeliveryState}).



%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

start_link(From, Notify, Channel, Reader, ConnConfig) ->
    gen_fsm:start_link(?MODULE, [From, Notify, Channel, Reader, ConnConfig], []).

-spec socket_ready(pid(), gen_tcp:socket()) -> ok.

socket_ready(Pid, Socket) ->
    gen_fsm:send_event(Pid, {socket_ready, Socket}).

%% -------------------------------------------------------------------
%% gen_fsm callbacks.
%% -------------------------------------------------------------------

init([From, Notify, Channel, Reader, ConnConfig]) ->
    amqp10_client_frame_reader:register_session(Reader, self(), Channel),
    State = #state{owner = From, channel = Channel, reader = Reader,
                   notify = Notify,
                   connection_config = ConnConfig},
    {ok, unmapped, State}.

unmapped({socket_ready, Socket}, State) ->
    State1 = State#state{socket = Socket},
    case send_begin(State1) of
        ok    -> {next_state, begin_sent, State1};
        Error -> {stop, Error, State1}
    end.

unmapped({attach, Attach}, From,
                      #state{early_attach_requests = EARs} = State) ->
    {next_state, unmapped,
     State#state{early_attach_requests = [{From, Attach} | EARs]}}.

begin_sent(#'v1_0.begin'{remote_channel = {ushort, RemoteChannel},
                         next_outgoing_id = {uint, NOI},
                         incoming_window = {uint, InWindow},
                         outgoing_window = {uint, OutWindow}
                        },
           #state{early_attach_requests = EARs} = State) ->

    State1 = State#state{remote_channel = RemoteChannel},
    State2 = lists:foldr(fun({From, Attach}, S) ->
                                 send_attach(fun send/2, Attach, From, S)
                         end, State1, EARs),

    ok = notify_session_begin(State2),

    {next_state, mapped, State2#state{early_attach_requests = [],
                                      next_incoming_id = NOI,
                                      remote_incoming_window = InWindow,
                                      remote_outgoing_window = OutWindow
                                     }}.

begin_sent({attach, Attach}, From,
                      #state{early_attach_requests = EARs} = State) ->
    {next_state, begin_sent,
     State#state{early_attach_requests = [{From, Attach} | EARs]}}.

mapped('end', State) ->
    %% We send the first end frame and wait for the reply.
    case send_end(State) of
        ok              -> {next_state, end_sent, State};
        {error, closed} -> {stop, normal, State};
        Error           -> {stop, Error, State}
    end;
mapped({flow, OutHandle, #'v1_0.flow'{link_credit = {uint, LinkCredit}} = Flow0},
       #state{links = Links,
              next_incoming_id = NII,
              next_outgoing_id = NOI,
              outgoing_window = OutWin,
              incoming_window = InWin} = State) ->
    #{OutHandle := #link{output_handle = H,
                         role = receiver,
                         delivery_count = DeliveryCount,
                         available = Available} = Link} = Links,
    Flow = Flow0#'v1_0.flow'{handle = uint(H),
                             next_incoming_id = uint(NII),
                             next_outgoing_id = uint(NOI),
                             outgoing_window = uint(OutWin),
                             incoming_window = uint(InWin),
                             delivery_count = uint(DeliveryCount),
                             available = uint(Available)
                             },
    ok = send(Flow, State),
    {next_state, mapped,
     State#state{links = Links#{OutHandle =>
                                Link#link{link_credit = LinkCredit}}}};

mapped(#'v1_0.end'{}, State) ->
    %% We receive the first end frame, reply and terminate.
    _ = send_end(State),
    % TODO: send notifications for links?
    {stop, normal, State};
mapped(#'v1_0.attach'{name = {utf8, Name},
                      initial_delivery_count = IDC,
                      handle = {uint, InHandle}},
        #state{links = Links, link_index = LinkIndex,
               link_handle_index = LHI,
               pending_attach_requests = PARs} = State) ->

    #{Name := From} = PARs,
    #{Name := OutHandle} = LinkIndex,
    #{OutHandle := Link0} = Links,
    gen_fsm:reply(From, {ok, OutHandle}),
    % TODO there coudl be many differnt things to do depending on link role
    DeliveryCount = case Link0 of
                        #link{role = sender, delivery_count = DC} -> DC;
                        _ -> unpack(IDC)
                    end,
    Link = Link0#link{input_handle = InHandle,
                      delivery_count = DeliveryCount},
    {next_state, mapped,
     State#state{links = Links#{OutHandle => Link},
                 link_handle_index = LHI#{InHandle => OutHandle},
                 pending_attach_requests = maps:remove(Name, PARs)}};

mapped(#'v1_0.flow'{handle = undefined} = Flow, State0) ->
    State = handle_session_flow(Flow, State0),
    {next_state, mapped, State};
mapped(#'v1_0.flow'{handle = {uint, InHandle}} = Flow,
       #state{links = Links} = State0) ->

    State = handle_session_flow(Flow, State0),

    {ok, #link{output_handle = OutHandle} = Link0} =
        find_link_by_input_handle(InHandle, State),

     % TODO: handle `send_flow` return tag
    {ok, Link} = handle_link_flow(Flow, Link0),
    Links1 = Links#{OutHandle => Link},
    State1 = State#state{links = Links1},
    {next_state, mapped, State1};

mapped({#'v1_0.transfer'{handle = {uint, InHandle},
                         more = true} = Transfer, Payload},
                         #state{links = Links} = State0) ->

    {ok, #link{output_handle = OutHandle} = Link} =
        find_link_by_input_handle(InHandle, State0),

    Link1 = append_partial_transfer(Transfer, Payload, Link),

    State = book_partial_transfer_received(
              State0#state{links = Links#{OutHandle => Link1}}),
    {next_state, mapped, State};
mapped({#'v1_0.transfer'{handle = {uint, InHandle},
                         delivery_id = {uint, DeliveryId},
                         settled = Settled} = Transfer0, Payload0},
                         #state{incoming_unsettled = Unsettled0} = State0) ->

    {ok, #link{target = {pid, TargetPid},
               output_handle = OutHandle} = Link} =
        find_link_by_input_handle(InHandle, State0),

    {Transfer, Payload} = complete_partial_transfer(Transfer0, Payload0, Link),

    Msg = decode_as_msg(Transfer, Payload),

    % deliver to the registered receiver process
    TargetPid ! {message, OutHandle, Msg},

    % stash the DeliveryId - not sure for what yet
    Unsettled = case Settled of
                   true -> Unsettled0;
                   _ -> Unsettled0#{DeliveryId => OutHandle}
                end,

    % link bookkeeping
    % TODO: what to do if LC-1 is negative?
    % detach the Link with a transfer-limit-exceeded error code
    State = book_transfer_received(State0#state{incoming_unsettled = Unsettled},
                                   Link),
    {next_state, mapped, State};
% role=true indicates the disposition is from a `receiver`. i.e. from the
% clients point of view these are dispositions relating to `sender`links
mapped(#'v1_0.disposition'{role = true, settled = true, first = {uint, First},
                           last = {uint, Last}, state = DeliveryState},
       #state{unsettled = Unsettled0} = State) ->

    % TODO: no good if the range becomes very large!!
    Unsettled =
        lists:foldl(fun(Id, Acc) ->
                            case Acc of
                                #{Id := {_Handle, Receiver}} ->
                                    S = translate_delivery_state(DeliveryState),
                                    gen_fsm:reply(Receiver, S),
                                    maps:remove(Id, Acc);
                                _ -> Acc
                            end
                    end, Unsettled0, lists:seq(First, Last)),

    {next_state, mapped, State#state{unsettled = Unsettled}};
mapped(Frame, State) ->
    error_logger:info_msg("UNHANDLED FRAME ~p~n", [Frame]),
    {next_state, mapped, State}.

%% mapped/3
%% TODO:: settled = false is probably dependent on the snd_settle_mode
%% e.g. if snd_settle_mode = settled it may not be valid to send an
%% unsettled transfer. Who knows really?
%%
%% Transfer. See spec section: 2.6.12
mapped({transfer, #'v1_0.transfer'{handle = {uint, OutHandle},
                                   settled = false} = Transfer0, Parts}, From,
       #state{next_delivery_id = NDI, links = Links} = State) ->

    case Links of
        #{OutHandle := #link{link_credit = LC}} when LC =< 0 ->
            {reply, insufficient_credit, mapped, State};
        #{OutHandle := Link} ->
            Transfer = Transfer0#'v1_0.transfer'{delivery_id = uint(NDI)},
            ok = send_transfer(Transfer, Parts, State),
            % delay reply to caller until disposition frame is received
            State1 = State#state{unsettled = #{NDI => {OutHandle, From}}},
            {next_state, mapped, book_transfer_send(Link, State1)};
        _ ->
            {reply, {error, link_not_found}, mapped, State}
    end;
mapped({transfer, #'v1_0.transfer'{handle = {uint, OutHandle}} = Transfer0,
        Parts}, _From, #state{next_delivery_id = NDI,
                              links = Links} = State) ->

    case Links of
        #{OutHandle := #link{link_credit = LC}} when LC =< 0 ->
            {reply, insufficient_credit, mapped, State};
        #{OutHandle := Link} ->
            Transfer = Transfer0#'v1_0.transfer'{delivery_id = uint(NDI)},
            ok = send_transfer(Transfer, Parts, State),
            % TODO look into if erlang will correctly wrap integers during
            % binary conversion.
            {reply, ok, mapped, book_transfer_send(Link, State)};
        _ ->
            {reply, {error, link_not_found}, mapped, State}
    end;

mapped({disposition, First, Last, Settled, DeliveryState}, _From,
       #state{incoming_unsettled = Unsettled0} = State) ->
    Disposition =
    begin
        DS = reverse_translate_delivery_state(DeliveryState),
        #'v1_0.disposition'{first = {uint, First},
                            last = {uint, Last},
                            settled = Settled,
                            state = DS}
    end,

    Unsettled = lists:foldl(fun maps:remove/2, Unsettled0,
                            lists:seq(First, Last)),
    Res = send(Disposition, State),

    {reply, Res, mapped, State#state{incoming_unsettled = Unsettled}};
mapped({attach, Attach}, From, State) ->
    State1 = send_attach(fun send/2, Attach, From, State),
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
                  next_outgoing_id = NextOutId,
                  incoming_window = InWin,
                  outgoing_window = OutWin} = State) ->
    Begin = #'v1_0.begin'{
               next_outgoing_id = uint(NextOutId),
               incoming_window = uint(InWin),
               outgoing_window = uint(OutWin)
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

send_transfer(Transfer0, Parts0, #state{socket = Socket, channel = Channel,
                                        connection_config = Config}) ->
    OutMaxFrameSize = case Config of
                          #{outgoing_max_frame_size := undefined} ->
                              ?MAX_MAX_FRAME_SIZE;
                          #{outgoing_max_frame_size := Sz} -> Sz;
                          _ -> ?MAX_MAX_FRAME_SIZE
                      end,
    Transfer = rabbit_amqp1_0_framing:encode_bin(Transfer0),
    TSize = iolist_size(Transfer),
    Parts = [rabbit_amqp1_0_framing:encode_bin(P) || P <- Parts0],
    PartsBin = iolist_to_binary(Parts),

    % TODO: this does not take the extended header into account
    % see: 2.3
    MaxPayloadSize = OutMaxFrameSize - TSize - ?FRAME_HEADER_SIZE,

    Frames = build_frames(Channel, Transfer0, PartsBin, MaxPayloadSize, []),
    ok = gen_tcp:send(Socket, Frames).

build_frames(Channel, Trf, Bin, MaxPayloadSize, Acc)
  when byte_size(Bin) =< MaxPayloadSize ->
    T = rabbit_amqp1_0_framing:encode_bin(Trf#'v1_0.transfer'{more = false}),
    Frame = rabbit_amqp1_0_binary_generator:build_frame(Channel, [T, Bin]),
    lists:reverse([Frame | Acc]);
build_frames(Channel, Trf, Payload, MaxPayloadSize, Acc) ->
    <<Bin:MaxPayloadSize/binary, Rest/binary>> = Payload,
    T = rabbit_amqp1_0_framing:encode_bin(Trf#'v1_0.transfer'{more = true}),
    Frame = rabbit_amqp1_0_binary_generator:build_frame(Channel, [T, Bin]),
    build_frames(Channel, Trf, Rest, MaxPayloadSize, [Frame | Acc]).

make_source(#{role := {sender, _}}) ->
    #'v1_0.source'{};
make_source(#{role := {receiver, #{address := Address}, _Pid}}) ->
    #'v1_0.source'{address = {utf8, Address}}.

make_target(#{role := {receiver, _Source, _Pid}}) ->
    #'v1_0.target'{};
make_target(#{role := {sender, #{address := Address}}}) ->
    #'v1_0.target'{address = {utf8, Address}}.

send_attach(Send, #{name := Name, role := Role} = Args, From,
      #state{next_link_handle = Handle, links = Links,
             pending_attach_requests = PARs,
             link_index = LinkIndex} = State) ->

    Source = make_source(Args),
    Target = make_target(Args),

    RoleAsBool = case Role of
                     {receiver, _, _} -> true;
                     _ -> false
                 end,

    % create attach performative
    Attach = #'v1_0.attach'{name = {utf8, Name},
                            role = RoleAsBool,
                            handle = {uint, Handle},
                            source = Source,
                            initial_delivery_count =
                                {uint, ?INITIAL_DELIVERY_COUNT},
                            snd_settle_mode = snd_settle_mode(Args),
                            rcv_settle_mode = rcv_settle_mode(Args),
                            target = Target},
    ok = Send(Attach, State),
    {T, S} = case Role of
                 {receiver, _, Pid} -> {{pid, Pid}, undefined};
                 {sender, #{address := TargetAddr}} ->
                     {TargetAddr, undefined}
             end,
    Link = #link{name = Name, output_handle = Handle,
                 role = element(1, Role), source = S, target = T},

    % stash the From pid
    State#state{links = Links#{Handle => Link},
                next_link_handle = Handle + 1,
                pending_attach_requests = PARs#{Name => From},
                link_index = LinkIndex#{Name => Handle}}.

-spec handle_session_flow(#'v1_0.flow'{}, #state{}) -> #state{}.
handle_session_flow(#'v1_0.flow'{next_incoming_id = MaybeNII,
                                 next_outgoing_id = {uint, NOI},
                                 incoming_window = {uint, InWin},
                                 outgoing_window = {uint, OutWin}},
       #state{next_outgoing_id = OurNOI} = State) ->

    NII = case MaybeNII of
              {uint, N} -> N;
              undefined -> ?INITIAL_OUTGOING_ID + 1 % TODO: should it be +1?
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

uint(Int) -> {uint, Int}.
unpack(X) -> amqp10_client_types:unpack(X).

snd_settle_mode(#{snd_settle_mode := unsettled}) -> {ubyte, 0};
snd_settle_mode(#{snd_settle_mode := settled}) -> {ubyte, 1};
snd_settle_mode(#{snd_settle_mode := mixed}) -> {ubyte, 2};
snd_settle_mode(_) -> undefined.

rcv_settle_mode(#{rcv_settle_mode := first}) -> {ubyte, 0};
rcv_settle_mode(#{rcv_settle_mode := second}) -> {ubyte, 1};
rcv_settle_mode(_) -> undefined.

translate_delivery_state(#'v1_0.accepted'{}) -> accepted;
translate_delivery_state(#'v1_0.rejected'{}) -> rejected;
translate_delivery_state(#'v1_0.modified'{}) -> modified;
translate_delivery_state(#'v1_0.released'{}) -> released;
translate_delivery_state(#'v1_0.received'{}) -> received.

reverse_translate_delivery_state(accepted) -> #'v1_0.accepted'{};
reverse_translate_delivery_state(rejected) -> #'v1_0.rejected'{};
reverse_translate_delivery_state(modified) -> #'v1_0.modified'{};
reverse_translate_delivery_state(released) -> #'v1_0.released'{};
reverse_translate_delivery_state(received) -> #'v1_0.received'{}.

notify_session_begin(#state{owner = Owner, notify = true}) ->
    Owner ! {session_begin, self()},
    ok;
notify_session_begin(_State) -> ok.

book_transfer_send(#link{output_handle = Handle} = Link,
              #state{next_outgoing_id = NOI,
                     next_delivery_id = NDI,
                     remote_incoming_window = RIW,
                     links = Links} = State) ->
    State#state{next_delivery_id = NDI+1,
                next_outgoing_id = NOI+1,
                remote_incoming_window = RIW-1,
                links = Links#{Handle => incr_link_counters(Link)}}.

book_partial_transfer_received(#state{next_incoming_id = NID,
                                      remote_outgoing_window = ROW} = State) ->
    State#state{next_incoming_id = NID+1,
                remote_outgoing_window = ROW-1}.

book_transfer_received(#state{next_incoming_id = NID,
                              remote_outgoing_window = ROW,
                              links = Links} = State,
                       #link{output_handle = OutHandle,
                             delivery_count = DC,
                             link_credit = LC} = Link) ->
    Link1 = Link#link{delivery_count = DC+1,
                      link_credit = LC-1},

    State#state{links = Links#{OutHandle => Link1},
                next_incoming_id = NID+1,
                remote_outgoing_window = ROW-1}.

incr_link_counters(#link{link_credit = LC, delivery_count = DC} = Link) ->
    Link#link{delivery_count = DC+1, link_credit = LC+1}.

append_partial_transfer(Transfer, Payload,
                        #link{partial_transfers = undefined} = Link) ->
    Link#link{partial_transfers = {Transfer, [Payload]}};
append_partial_transfer(_Transfer, Payload,
                        #link{partial_transfers = {T, Payloads}} = Link) ->
    Link#link{partial_transfers = {T, [Payload | Payloads]}}.

complete_partial_transfer(Transfer, Payload,
                          #link{partial_transfers = undefined}) ->
    {Transfer, Payload};
complete_partial_transfer(_Transfer, Payload,
                          #link{partial_transfers = {T, Payloads}}) ->
    {T, iolist_to_binary(lists:reverse([Payload | Payloads]))}.

decode_as_msg(Transfer, Payload) ->
    Records = rabbit_amqp1_0_framing:decode_bin(Payload),
    amqp10_msg:from_amqp_records([Transfer | Records]).

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

-endif.
