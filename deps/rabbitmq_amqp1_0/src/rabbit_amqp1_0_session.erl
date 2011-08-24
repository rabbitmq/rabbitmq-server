-module(rabbit_amqp1_0_session).

-export([process_frame/2, maybe_init_publish_id/2, record_publish/3,
         incr_transfer_number/1, next_transfer_number/1, may_send/1,
         record_delivery/3]).

-include("rabbit_amqp1_0_session.hrl").

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


