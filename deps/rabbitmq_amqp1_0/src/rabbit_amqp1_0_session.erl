-module(rabbit_amqp1_0_session).

-export([process_frame/2]).

-export([init/1, begin_/2, maybe_init_publish_id/2, record_publish/3,
         incr_transfer_number/1, next_transfer_number/1, may_send/1,
         record_delivery/3, settle/3, flow_fields/2, channel/1, flow/2, ack/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

%% TODO test where the sweetspot for gb_trees is
-define(MAX_SESSION_BUFFER_SIZE, 4096).
-define(DEFAULT_MAX_HANDLE, 16#ffffffff).

-record(session, {channel_num, %% we just use the incoming (AMQP 1.0) channel number
                  next_transfer_number = 0, % next outgoing id
                  max_outgoing_id, % based on the remote incoming window size
                  next_incoming_id, % just to keep a check
                  next_publish_id, %% the 0-9-1-side counter for confirms
                  %% we make incoming and outgoing session buffers the
                  %% same size
                  window_size,
                  ack_counter = 0,
                  incoming_unsettled_map,
                  outgoing_unsettled_map }).

-record(outgoing_transfer, {delivery_tag, expected_outcome}).

process_frame(Pid, Frame) ->
    gen_server2:cast(Pid, {frame, Frame}).

init(Channel) ->
    #session{channel_num            = Channel,
             next_publish_id        = 0,
             ack_counter            = 0,
             incoming_unsettled_map = gb_trees:empty(),
             outgoing_unsettled_map = gb_trees:empty()}.

%% Session window:
%%
%% Each session has two buffers, one to record the unsettled state of
%% incoming messages, one to record the unsettled state of outgoing
%% messages.  In general we want to bound these buffers; but if we
%% bound them, and don't tell the other side, we may end up
%% deadlocking the other party.
%%
%% Hence the flow frame contains a session window, expressed as the
%% next-id and the window size for each of the buffers. The frame
%% refers to the buffers of the sender of the frame, of course.
%%
%% The numbers work this way: for the outgoing buffer, the next-id is
%% the next transfer id the session will send, and it will stop
%% sending at next-id + window.  For the incoming buffer, the next-id
%% is the next transfer id expected, and it will not accept messages
%% beyond next-id + window (in fact it will probably close the
%% session, since sending outside the window is a transgression of the
%% protocol).
%%
%% Usually we will want to base our incoming window size on the other
%% party's outgoing window size (given in begin{}), since we will
%% never need more state than they are keeping (they'll stop sending
%% before that happens), subject to a maximum.  Similarly the outgoing
%% window, on the basis that the other party is likely to make its
%% buffers the same size (or that's our best guess).
%%
%% Note that we will occasionally overestimate these buffers, because
%% the far side may be using a circular buffer, in which case they
%% care about the distance from the low water mark (i.e., the least
%% transfer for which they have unsettled state) rather than the
%% number of entries.
%%
%% We use ordered sets for our buffers, which means we care about the
%% total number of entries, rather than the smallest entry. Thus, our
%% window will always be, by definition, BOUND - TOTAL.
begin_(#'v1_0.begin'{next_outgoing_id = {uint, RemoteNextIn},
                     incoming_window  = RemoteInWindow,
                     outgoing_window  = RemoteOutWindow,
                     handle_max       = HandleMax0},
       Session = #session{next_transfer_number = LocalNextOut,
                          channel_num          = Channel}) ->
    Window = case RemoteInWindow of
                 {uint, Size} -> Size;
                 undefined    -> ?MAX_SESSION_BUFFER_SIZE
             end,
    %% TODO does it make sense to have two different sizes
    SessionBufferSize = erlang:min(Window, ?MAX_SESSION_BUFFER_SIZE),
    HandleMax = case HandleMax0 of
                    {uint, Max} -> Max;
                    _ -> ?DEFAULT_MAX_HANDLE
                end,
    {ok, #'v1_0.begin'{remote_channel = {ushort, Channel},
                       handle_max = {uint, HandleMax},
                       next_outgoing_id = {uint, LocalNextOut},
                       incoming_window = {uint, SessionBufferSize},
                       outgoing_window = {uint, SessionBufferSize}},
     Session#session{
       next_incoming_id = RemoteNextIn,
       max_outgoing_id  = rabbit_misc:serial_add(RemoteNextIn, Window),
       window_size      = SessionBufferSize},
     SessionBufferSize}.

maybe_init_publish_id(false, Session) ->
    Session;
maybe_init_publish_id(true, Session = #session{next_publish_id = Id}) ->
    Session#session{next_publish_id = erlang:max(1, Id)}.

record_publish(Settled, TxfrId,
        Session = #session{next_publish_id        = Id,
                           incoming_unsettled_map = Unsettled}) ->
    Id1 = case Id of
              0 -> 0;
              _ -> Id + 1 % serial?
          end,
    Unsettled1 = case Settled of
                     true  -> Unsettled;
                     %% Be lenient -- this is a boolean and really ought
                     %% to have a value, but the spec doesn't currently
                     %% require it.
                     Symbol when
                           Symbol =:= false orelse
                           Symbol =:= undefined ->
                         gb_trees:insert(Id,
                                         TxfrId,
                                         Unsettled)
                 end,
    incr_transfer_number(Session#session{next_publish_id        = Id1,
                                         incoming_unsettled_map = Unsettled1}).

incr_transfer_number(Session = #session{next_transfer_number = Num}) ->
    %% TODO this should be a serial number
    Session#session{next_transfer_number = rabbit_misc:serial_add(Num, 1)}.

next_transfer_number(#session{next_transfer_number = Num}) -> Num.

may_send(#session{next_transfer_number   = TransferNumber,
                  max_outgoing_id        = LocalMaxOut,
                  window_size            = WindowSize,
                  outgoing_unsettled_map = Unsettled}) ->
    %% FIXME
    %% If either the outgoing session window, or the remote incoming
    %% session window, is closed, we can't send this. This probably
    %% happened because the far side is basing its window on the low
    %% water mark, whereas we can only tell the queue to have at most
    %% "prefetch_count" messages in flight (i.e., a total). For the
    %% minute we will have to just break things.
    NumUnsettled = gb_trees:size(Unsettled),
    (LocalMaxOut > TransferNumber) andalso (WindowSize >= NumUnsettled).

record_delivery(DeliveryTag, DefaultOutcome,
                Session = #session{next_transfer_number   = TransferNumber,
                                   outgoing_unsettled_map = Unsettled}) ->
    Unsettled1 = gb_trees:insert(TransferNumber,
                                 #outgoing_transfer{
                                   delivery_tag     = DeliveryTag,
                                   expected_outcome = DefaultOutcome },
                                 Unsettled),
    Session#session{outgoing_unsettled_map = Unsettled1}.

%% We've been told that the fate of a transfer has been determined.
%% Generally if the other side has not settled it, we will do so.  If
%% the other side /has/ settled it, we don't need to reply -- it's
%% already forgotten its state for the transfer anyway.
settle(Disp = #'v1_0.disposition'{first   = First0,
                                  last    = Last0,
                                  settled = Settled},
       Session = #session{outgoing_unsettled_map = Unsettled},
       UpstreamAckFun) ->
    {uint, First} = First0,
    %% Last may be omitted, in which case it's the same as first
    Last = case Last0 of
               {uint, L} -> L;
               undefined -> First
           end,
    %% The other party may be talking about something we've already
    %% forgotten; this isn't a crime, we can just ignore it.
    case gb_trees:is_empty(Unsettled) of
        true ->
            {none, Session};
        false ->
            {LWM, _} = gb_trees:smallest(Unsettled),
            {HWM, _} = gb_trees:largest(Unsettled),
            if Last < LWM ->
                    {none, Session};
               %% FIXME this should probably be an error, rather than ignored.
               First > HWM ->
                    {none, Session};
               true ->
                    Unsettled1 =
                        lists:foldl(
                          fun (Transfer, Map) ->
                                  case gb_trees:lookup(Transfer, Map) of
                                      none ->
                                          Map;
                                      {value, Entry} ->
                                          ?DEBUG("Settling ~p with ~p~n", [Transfer, Outcome]),
                                          #outgoing_transfer{ delivery_tag = DeliveryTag } = Entry,
                                          UpstreamAckFun(DeliveryTag),
                                          gb_trees:delete(Transfer, Map)
                                  end
                          end,
                          Unsettled, lists:seq(erlang:max(LWM, First),
                                               erlang:min(HWM, Last))),
                    {case Settled of
                         true  -> none;
                         false -> Disp#'v1_0.disposition'{ settled = true,
                                                           role = ?SEND_ROLE }
                     end,
                     Session#session{outgoing_unsettled_map = Unsettled1}}
            end
    end.

flow_fields(Frames, Session) when is_list(Frames) ->
    [flow_fields(F, Session) || F <- Frames];

flow_fields(Flow = #'v1_0.flow'{},
             #session{next_transfer_number = NextOut,
                      next_incoming_id = NextIn,
                      window_size = Window,
                      outgoing_unsettled_map = UnsettledOut,
                      incoming_unsettled_map = UnsettledIn }) ->
    Flow#'v1_0.flow'{
      next_outgoing_id = {uint, NextOut},
      outgoing_window = {uint, Window - gb_trees:size(UnsettledOut)},
      next_incoming_id = {uint, NextIn},
      incoming_window = {uint, Window - gb_trees:size(UnsettledIn)}};

flow_fields(Frame, _Session) ->
    Frame.

channel(#session{channel_num = Channel}) -> Channel.

%% See above regarding the session window. We should already know the
%% next outgoing transfer sequence number, because it's one more than
%% the last transfer we saw; and, we don't need to know the next
%% incoming transfer sequence number (although we might use it to
%% detect congestion -- e.g., if it's lagging far behind our outgoing
%% sequence number). We probably care about the outgoing window, since
%% we want to keep it open by sending back settlements, but there's
%% not much we can do to hurry things along.
%%
%% We do care about the incoming window, because we must not send
%% beyond it. This may cause us problems, even in normal operation,
%% since we want our unsettled transfers to be exactly those that are
%% held as unacked by the backing channel; however, the far side may
%% close the window while we still have messages pending
%% transfer. Note that this isn't a race so far as AMQP 1.0 is
%% concerned; it's only because AMQP 0-9-1 defines QoS in terms of the
%% total number of unacked messages, whereas 1.0 has an explicit window.
flow(#'v1_0.flow'{next_incoming_id = RemoteNextIn0,
                  incoming_window  = {uint, RemoteWindowIn},
                  next_outgoing_id = {uint, RemoteNextOut},
                  outgoing_window  = {uint, RemoteWindowOut}},
     Session = #session{next_incoming_id     = LocalNextIn,
                        max_outgoing_id      = _LocalMaxOut,
                        next_transfer_number = LocalNextOut}) ->
    %% Check the things that we know for sure
    %% TODO sequence number comparisons
    ?DEBUG("~p == ~p~n", [RemoteNextOut, LocalNextIn]),
    %% TODO the Python client sets next_outgoing_id=2 on begin, then sends a
    %% flow with next_outgoing_id=1. Not sure what that's meant to mean.
    %% RemoteNextOut = LocalNextIn,
    %% The far side may not have our begin{} with our next-transfer-id
    RemoteNextIn = case RemoteNextIn0 of
                       {uint, Id} -> Id;
                       undefined  -> LocalNextOut
                   end,
    ?DEBUG("~p =< ~p~n", [RemoteNextIn, LocalNextOut]),
    true = (RemoteNextIn =< LocalNextOut),
    %% Adjust our window
    RemoteMaxOut = RemoteNextIn + RemoteWindowIn,
    Session#session{max_outgoing_id = RemoteMaxOut}.

%% An acknowledgement from the queue.  To keep the incoming window
%% moving, we make sure to update them with the session counters every
%% once in a while.  Assuming that the buffer is an appropriate size,
%% about once every window_size / 2 is a good heuristic.
ack(#'basic.ack'{delivery_tag = DTag, multiple = Multiple},
    Session = #session{incoming_unsettled_map = Unsettled,
                       window_size            = Window,
                       ack_counter            = AckCounter}) ->
    {TransferIds, Unsettled1} =
        case Multiple of
            true  -> acknowledgement_range(DTag, Unsettled);
            false -> case gb_trees:lookup(DTag, Unsettled) of
                         {value, Id} ->
                             {[Id], gb_trees:delete(DTag, Unsettled)};
                         none ->
                             {[], Unsettled}
                     end
        end,
    Disposition = case TransferIds of
                      [] -> [];
                      _  -> [acknowledgement(
                               TransferIds,
                               #'v1_0.disposition'{role = ?RECV_ROLE})]
    end,
    HalfWin = Window div 2,
    {AckCounter1, Flow} =
        case AckCounter + length(TransferIds) of
            Over when Over >= HalfWin -> {Over - HalfWin, [#'v1_0.flow'{}]};
            Counter                   -> {Counter,        []}
        end,
    {Disposition ++ Flow,
     Session#session{ack_counter            = AckCounter1,
                     incoming_unsettled_map = Unsettled1}}.

acknowledgement_range(DTag, Unsettled) ->
    acknowledgement_range(DTag, Unsettled, []).

acknowledgement_range(DTag, Unsettled, Acc) ->
    case gb_trees:is_empty(Unsettled) of
        true ->
            {lists:reverse(Acc), Unsettled};
        false ->
            {DTag1, TransferId} = gb_trees:smallest(Unsettled),
            case DTag1 =< DTag of
                true ->
                    {_K, _V, Unsettled1} = gb_trees:take_smallest(Unsettled),
                    acknowledgement_range(DTag, Unsettled1,
                                          [TransferId|Acc]);
                false ->
                    {lists:reverse(Acc), Unsettled}
            end
    end.

acknowledgement(TransferIds, Disposition) ->
    Disposition#'v1_0.disposition'{ first = {uint, hd(TransferIds)},
                                    last = {uint, lists:last(TransferIds)},
                                    settled = true,
                                    state = #'v1_0.accepted'{} }.
