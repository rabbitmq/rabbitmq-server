-module(rabbit_amqp1_0_session).

-export([process_frame/2]).

-export([init/1, begin_/2, maybe_init_publish_id/2, record_publish/3,
         incr_transfer_number/1, next_transfer_number/1, may_send/1,
         record_delivery/3, settle/3, flow_fields/2, flow_fields/1]).

-include("rabbit_amqp1_0.hrl").
-include("rabbit_amqp1_0_session.hrl").

%% TODO test where the sweetspot for gb_trees is
-define(MAX_SESSION_BUFFER_SIZE, 4096).
-define(DEFAULT_MAX_HANDLE, 16#ffffffff).

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

flow_fields(Session) ->
    flow_fields(#'v1_0.flow'{}, Session).

