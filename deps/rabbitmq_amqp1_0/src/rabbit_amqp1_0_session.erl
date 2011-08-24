-module(rabbit_amqp1_0_session).

-export([process_frame/2, maybe_init_publish_id/2, record_publish/3,
         incr_transfer_number/1, next_transfer_number/1, may_send/1,
         record_delivery/3, settle/3, flow_fields/2, flow_fields/1]).

-include("rabbit_amqp1_0.hrl").
-include("rabbit_amqp1_0_session.hrl").

-record(outgoing_transfer, {delivery_tag, expected_outcome}).

process_frame(Pid, Frame) ->
    gen_server2:cast(Pid, {frame, Frame}).

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

flow_fields(Frames, Session) ->
    [flow_fields0(F, Session) || F <- Frames].

flow_fields(Session) ->
    flow_fields0(#'v1_0.flow'{}, Session).

flow_fields0(Flow = #'v1_0.flow'{},
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
flow_fields0(Frame, _Session) ->
    Frame.
