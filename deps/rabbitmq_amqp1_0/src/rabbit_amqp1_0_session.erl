-module(rabbit_amqp1_0_session).

-export([process_frame/2, maybe_init_publish_id/2, publish/3]).

-include("rabbit_amqp1_0_session.hrl").

process_frame(Pid, Frame) ->
    gen_server2:cast(Pid, {frame, Frame}).

maybe_init_publish_id(false, Session) ->
    Session;
maybe_init_publish_id(true, Session = #session{next_publish_id = Id}) ->
    Session#session{next_publish_id = erlang:max(1, Id)}.

publish(Settled, TxfrId,
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
    Session#session{next_publish_id        = Id1,
                    next_transfer_number   = next_transfer_number(TxfrId),
                    incoming_unsettled_map = Unsettled1}.

next_transfer_number(TransferNumber) ->
    %% TODO this should be a serial number
    rabbit_misc:serial_add(TransferNumber, 1).
