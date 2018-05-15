-module(rabbit_classic_queue).
-behaviour(rabbit_queue).

-export([
         init_state/2,
         ack/4,
         reject/5,
         basic_get/6,
         declare/2,
         deliver/3,
         delivery_target/2,
         delete/4,
         stat/1
         ]).

-record(state, {}).

-opaque state() :: #state{}.

-export_type([
              state/0
              ]).


-include_lib("rabbit_common/include/rabbit.hrl").

init_state(_, _) ->
    undefined.

ack(QPid, _CTag, MsgIds, QState) ->
    ok = delegate:invoke_no_result(QPid,
                                   {gen_server2, cast, [{ack, MsgIds, self()}]}),
    QState.

reject(QPid, _CTag, Requeue, MsgIds, QStates) ->
    delegate:invoke_no_result(QPid, {gen_server2, cast,
                                     [{reject, Requeue, MsgIds, self()}]}),
    QStates.

basic_get(#amqqueue{pid = QPid}, ChPid, NoAck, LimiterPid, _,
          undefined) ->
    case delegate:invoke(QPid, {gen_server2, call,
                                [{basic_get, ChPid, NoAck, LimiterPid}, infinity]}) of
        {ok, Count, Msg} ->
            {ok, Count, Msg, undefined};
        empty ->
            {empty, undefined}
    end.

declare(#amqqueue{name = QName, vhost = VHost} = Q, Node) ->
    Node1 = case rabbit_queue_master_location_misc:get_location(Q)  of
              {ok, Node0}  -> Node0;
              {error, _}   -> Node
            end,
    Node1 = rabbit_mirror_queue_misc:initial_queue_node(Q, Node1),
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost, Node1) of
        {ok, _} ->
            gen_server2:call(
              rabbit_amqqueue_sup_sup:start_queue_process(Node1, Q, declare),
              {init, new}, infinity);
        {error, Error} ->
            rabbit_misc:protocol_error(internal_error,
                            "Cannot declare a queue '~s' on node '~s': ~255p",
                            [rabbit_misc:rs(QName), Node1, Error])
    end.


delivery_target(#amqqueue{pid = QPid, slave_pids = SPids}, Acc) ->
    maps:update_with(?MODULE,
                     fun ({MPidsAcc, SPidsAcc}) ->
                             {[QPid | MPidsAcc], SPids ++ SPidsAcc}
                     end, {[QPid], SPids}, Acc).

deliver({MPids, SPids}, #delivery{flow = Flow} = Delivery, _) ->
    QPids = MPids ++ SPids,
    %% We use up two credits to send to a slave since the message
    %% arrives at the slave from two directions. We will ack one when
    %% the slave receives the message direct from the channel, and the
    %% other when it receives it via GM.

    %% TODO what to do with credit flow for quorum queues?
    case Flow of
        %% Here we are tracking messages sent by the rabbit_channel
        %% process. We are accessing the rabbit_channel process
        %% dictionary.
        flow   -> [credit_flow:send(QPid) || QPid <- QPids],
                  [credit_flow:send(QPid) || QPid <- SPids];
        noflow -> ok
    end,

    %% We let slaves know that they were being addressed as slaves at
    %% the time - if they receive such a message from the channel
    %% after they have become master they should mark the message as
    %% 'delivered' since they do not know what the master may have
    %% done with it.
    MMsg = {deliver, Delivery, false},
    SMsg = {deliver, Delivery, true},
    delegate:invoke_no_result(MPids, {gen_server2, cast, [MMsg]}),
    delegate:invoke_no_result(SPids, {gen_server2, cast, [SMsg]}),
    undefined.

delete(Q, IfUnused, IfEmpty, ActingUser) ->
    case wait_for_promoted_or_stopped(Q) of
        {promoted, #amqqueue{pid = QPid}} ->
            delegate:invoke(QPid, {gen_server2, call, [{delete, IfUnused, IfEmpty, ActingUser}, infinity]});
        {stopped, Q1} ->
            #resource{name = Name, virtual_host = Vhost} = Q1#amqqueue.name,
            case IfEmpty of
                true ->
                    rabbit_log:error("Queue ~s in vhost ~s has its master node down and "
                                     "no mirrors available or eligible for promotion. "
                                     "The queue may be non-empty. "
                                     "Refusing to force-delete.",
                                     [Name, Vhost]),
                    {error, not_empty};
                false ->
                    rabbit_log:warning("Queue ~s in vhost ~s has its master node is down and "
                                       "no mirrors available or eligible for promotion. "
                                       "Forcing queue deletion.",
                                       [Name, Vhost]),
                    delete_crashed_internal(Q1, ActingUser),
                    {ok, 0}
            end;
        {error, not_found} ->
            %% Assume the queue was deleted
            {ok, 0}
    end.

-spec stat(rabbit_types:amqqueue()) -> {ok, non_neg_integer(), non_neg_integer()}.
stat(#amqqueue{pid = QPid}) ->
    delegate:invoke(QPid, {gen_server2, call, [stat, infinity]}).

delete_crashed_internal(Q = #amqqueue{ name = QName }, ActingUser) ->
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    BQ:delete_crashed(Q),
    ok = internal_delete(QName, ActingUser).

internal_delete1(QueueName, OnlyDurable) ->
    ok = mnesia:delete({rabbit_queue, QueueName}),
    mnesia:delete({rabbit_durable_queue, QueueName}),
    %% we want to execute some things, as decided by rabbit_exchange,
    %% after the transaction.
    rabbit_binding:remove_for_destination(QueueName, OnlyDurable).

internal_delete(QueueName, ActingUser) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case {mnesia:wread({rabbit_queue, QueueName}),
                    mnesia:wread({rabbit_durable_queue, QueueName})} of
                  {[], []} ->
                      rabbit_misc:const({error, not_found});
                  _ ->
                      Deletions = internal_delete1(QueueName, false),
                      T = rabbit_binding:process_deletions(Deletions,
                                                           ?INTERNAL_USER),
                      fun() ->
                              ok = T(),
                              rabbit_core_metrics:queue_deleted(QueueName),
                              ok = rabbit_event:notify(
                                     queue_deleted,
                                     [{name, QueueName},
                                      {user_who_performed_action, ActingUser}])
                      end
              end
      end).

wait_for_promoted_or_stopped(#amqqueue{name = QName}) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q = #amqqueue{pid = QPid, slave_pids = SPids}} ->
            case rabbit_mnesia:is_process_alive(QPid) of
                true  -> {promoted, Q};
                false ->
                    case lists:any(fun(Pid) ->
                                       rabbit_mnesia:is_process_alive(Pid)
                                   end, SPids) of
                        %% There is a live slave. May be promoted
                        true ->
                            timer:sleep(100),
                            wait_for_promoted_or_stopped(Q);
                        %% All slave pids are stopped.
                        %% No process left for the queue
                        false -> {stopped, Q}
                    end
            end;
        {error, not_found} ->
            {error, not_found}
    end.
