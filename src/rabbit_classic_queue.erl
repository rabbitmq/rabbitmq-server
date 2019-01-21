-module(rabbit_classic_queue).
-behavior(rabbit_queue_type).

-include_lib("rabbit_common/include/rabbit.hrl").
-export([
         init/1,
         in/4,
         handle_queue_info/3,
         handle_down/4,
         begin_receive/4,
         end_receive/3,
         update_msg_state/6
        ]).

-record(?MODULE, {queue :: #amqqueue{}}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

init(Q) ->
    %% TODO: monitor pid
    {#?MODULE{queue = Q}, []}.

in(QId, #?MODULE{queue = Q} = State, _Seq, Delivery) ->
    Msg = {deliver, Delivery#delivery{queue_id = QId}, false},
    ok = gen_server2:cast(Q#amqqueue.pid, Msg),
    {State, []}.


handle_queue_info(_QId, State, {confirm, Seqs, _QueuePid}) ->
    {State, [{msg_state_update, accepted, Seqs}]};
handle_queue_info(_QId, State, {deliver, Tag, AckRequired, Msg}) ->
    {State, [{deliveries, [{Tag, AckRequired, Msg}]}]}.


handle_down(_QId, State, _Ref, _Reason) ->
    {State, []}.

begin_receive(QId, State, ConsumerTag,
              #{acting_user := ActingUser,
                ok_msg := OkMsg,
                limiter := Limiter} = Meta) ->
    Args = maps:get(consumer_args, Meta, []),
    %% TODO: validate consume arguments
    {Prefetch, NoAck} =
        case Meta of
            #{credit := {simple_prefetch, P}} ->
                {P, false};
            #{credit := none} ->
                {0, true}
        end,

    ExclusiveConsume = false,
    QPid = State#?MODULE.queue#amqqueue.pid,
    ChPid = self(),
    LimiterPid = rabbit_limiter:pid(Limiter),
    LimiterActive = rabbit_limiter:is_active(Limiter),
    case delegate:invoke(QPid,
                         {gen_server2, call,
                          [{basic_consume, NoAck, ChPid, QId, LimiterPid,
                            LimiterActive,
                            Prefetch, ConsumerTag,
                            ExclusiveConsume,
                            Args, OkMsg, ActingUser}, infinity]}) of
        ok ->
            {ok, State, []};
        Err ->
            Err
    end.

end_receive(_QId, State, _Tag) ->
    {State, []}.

update_msg_state(_QId, State, _ReceiveTag, MsgIds, accepted, true = _IsSettled) ->
    QPid = State#?MODULE.queue#amqqueue.pid,
    ChPid = self(),
    _ = delegate:invoke_no_result(QPid, {gen_server2, cast,
                                         [{ack, MsgIds, ChPid}]}),
    {State, []}.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
