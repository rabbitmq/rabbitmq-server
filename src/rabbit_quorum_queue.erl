%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_quorum_queue).

-export([init_state/1, init_state/2, handle_event/2]).
-export([declare/1, recover/1, stop/1, delete/4, delete_immediately/1]).
-export([info/1, info/2, stat/1, infos/1]).
-export([ack/3, reject/4, basic_get/4, basic_consume/9]).
-export([stateless_deliver/2, deliver/3]).
-export([dead_letter_publish/5]).
-export([queue_name/1]).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

-type ra_node_id() :: {Name :: atom(), Node :: node()}.
-type msg_id() :: non_neg_integer().
-type qmsg() :: {rabbit_types:r('queue'), pid(), msg_id(), boolean(), rabbit_types:message()}.

-spec init_state(ra_node_id()) -> ra_fifo_client:state().
-spec handle_event({'ra_event', ra_node_id(), any()}, ra_fifo_client:state()) ->
                          {'internal', Correlators :: [term()], ra_fifo_client:state()} |
                          {ra_fifo:client_msg(), ra_fifo_client:state()}.
-spec declare(rabbit_types:amqqueue()) -> {'new', rabbit_types:amqqueue(), ra_fifo_client:state()}.
-spec recover([rabbit_types:amqqueue()]) -> [rabbit_types:amqqueue() |
                                             {'absent', rabbit_types:amqqueue(), atom()}].
-spec stop(rabbit_types:vhost()) -> 'ok'.
-spec delete(rabbit_types:amqqueue(), boolean(), boolean(), rabbit_types:username()) ->
                    {'ok', QLen :: non_neg_integer()}.
-spec ack(rabbit_types:ctag(), [msg_id()], ra_fifo_client:state()) ->
                 {'ok', ra_fifo_client:state()}.
-spec reject(Confirm :: boolean(), rabbit_types:ctag(), [msg_id()], ra_fifo_client:state()) ->
                    {'ok', ra_fifo_client:state()}.
-spec basic_get(rabbit_types:amqqueue(), NoAck :: boolean(), rabbit_types:ctag(),
                ra_fifo_client:state()) ->
                       {'ok', 'empty', ra_fifo_client:state()} |
                       {'ok', QLen :: non_neg_integer(), qmsg(), ra_fifo_client:state()}.
-spec basic_consume(rabbit_types:amqqueue(), NoAck :: boolean(), ChPid :: pid(),
                    ConsumerPrefetchCount :: non_neg_integer(), rabbit_types:ctag(),
                    ExclusiveConsume :: boolean(), Args :: rabbit_framing:amqp_table(),
                    any(), ra_fifo_client:state()) -> {'ok', ra_fifo_client:state()}.
-spec stateless_deliver(ra_node_id(), rabbit_types:delivery()) -> 'ok'.
-spec deliver(Confirm :: boolean(), rabbit_types:delivery(), ra_fifo_client:state()) ->
                     ra_fifo_client:state().
-spec info(rabbit_types:amqqueue()) -> rabbit_types:infos().
-spec info(rabbit_types:amqqueue(), rabbit_types:info_keys()) -> rabbit_types:infos().
-spec infos(rabbit_types:r('queue')) -> rabbit_types:infos().
-spec stat(rabbit_types:amqqueue()) -> {'ok', non_neg_integer(), non_neg_integer()}.

-define(STATISTICS_KEYS,
        [policy,
         operator_policy,
         effective_policy_definition,
         exclusive_consumer_pid,
         exclusive_consumer_tag,
         consumers,
         consumer_utilisation,
         memory,
         state,
         garbage_collection
        ]).

%%----------------------------------------------------------------------------

init_state({Name, _} = Id) ->
    ra_fifo_client:init(Name, [Id]).

init_state({Name, _} = Id, QName) ->
    %% The quorum mapping entry is created on queue declare, but if another node
    %% has processed this command the actual node won't have the entry.
    %% init_state/1 can be used when there is guarantee this has been called, such
    %% as when processing ack's and rejects. 
    ets:insert_new(quorum_mapping, {Name, QName}),
    ra_fifo_client:init(Name, [Id]).

handle_event({ra_event, From, Evt}, FState) ->
    ra_fifo_client:handle_ra_event(From, Evt, FState).

declare(#amqqueue{name = QName,
                  durable = Durable,
                  auto_delete = AutoDelete,
                  arguments = Arguments,
                  exclusive_owner = ExclusiveOwner,
                  options = Opts} = Q) ->
    check_invalid_arguments(QName, Arguments),
    RaName = qname_to_rname(QName),
    Id = {RaName, node()},
    Nodes = rabbit_mnesia:cluster_nodes(all),
    NewQ = Q#amqqueue{pid = Id,
                      quorum_nodes = Nodes},
    ets:insert(quorum_mapping, {RaName, QName}),
    RaMachine = {module, ra_fifo,
                 #{dead_letter_handler => dlx_mfa(NewQ)}},
    {ok, _Started, _NotStarted} = ra:start_cluster(RaName, RaMachine,
                                                   [{RaName, Node} || Node <- Nodes]),
    FState = init_state(Id),
    _ = rabbit_amqqueue:internal_declare(NewQ, false),
    OwnerPid = case ExclusiveOwner of
                   none -> '';
                   _ -> ExclusiveOwner
               end,
    %% TODO do the quorum queue receive the `force_event_refresh`? what do we do with it?
    rabbit_event:notify(queue_created,
                        [{name, QName},
                         {durable, Durable},
                         {auto_delete, AutoDelete},
                         {arguments, Arguments},
                         {owner_pid, OwnerPid},
                         {exclusive, is_pid(ExclusiveOwner)},
                         {user_who_performed_action, maps:get(user, Opts, ?UNKNOWN_USER)}]),
    {new, NewQ, FState}.

recover(Queues) ->
    [begin
         ets:insert_new(quorum_mapping, {Name, QName}),
         ok = ra:restart_node({Name, node()}),
         rabbit_amqqueue:internal_declare(Q, true)
     end || #amqqueue{name = QName, pid = {Name, _}} = Q <- Queues].

stop(VHost) ->
    _ = [ra:stop_node(Pid) || #amqqueue{pid = Pid} <- find_quorum_queues(VHost)],
    ok.

delete(#amqqueue{ type = quorum, pid = {Name, _}, name = QName, quorum_nodes = QNodes},
       _IfUnused, _IfEmpty, ActingUser) ->
    %% TODO Quorum queue needs to support consumer tracking for IfUnused
    Msgs = quorum_messages(Name),
    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
    {ok, Leader} = ra:delete_cluster([{Name, Node} || Node <- QNodes]),
    MRef = erlang:monitor(process, Leader),
    receive
        {'DOWN', MRef, process, _, _} ->
            ok
    end,
    rabbit_core_metrics:queue_deleted(QName),
    ets:delete(quorum_mapping, Name),
    {ok, Msgs}.

delete_immediately({Name, _} = QPid) ->
    QName = queue_name(Name),
    _ = rabbit_amqqueue:internal_delete(QName, ?INTERNAL_USER),
    ok = ra:delete_cluster([QPid]),
    rabbit_core_metrics:queue_deleted(QName),
    ets:delete(quorum_mapping, Name),
    ok.

ack(CTag, MsgIds, FState) ->
    ra_fifo_client:settle(quorum_ctag(CTag), MsgIds, FState).

reject(true, CTag, MsgIds, FState) ->
    ra_fifo_client:return(quorum_ctag(CTag), MsgIds, FState);
reject(false, CTag, MsgIds, FState) ->
    ra_fifo_client:discard(quorum_ctag(CTag), MsgIds, FState).

basic_get(#amqqueue{name = QName, pid = {Name, _} = Id, type = quorum}, NoAck,
          CTag0, FState0) ->
    CTag = quorum_ctag(CTag0),
    Settlement = case NoAck of
                     true ->
                         settled;
                     false ->
                         unsettled
                 end,
    case ra_fifo_client:dequeue(CTag, Settlement, FState0) of
        {ok, empty, FState} ->
            {ok, empty, FState};
        {ok, {MsgId, {MsgHeader, Msg}}, FState} ->
            IsDelivered = maps:is_key(delivery_count, MsgHeader),
            {ok, quorum_messages(Name), {QName, Id, MsgId, IsDelivered, Msg}, FState}
    end.

basic_consume(#amqqueue{name = QName, type = quorum}, NoAck, ChPid,
              ConsumerPrefetchCount, ConsumerTag, ExclusiveConsume, Args, OkMsg, FState0) ->
    maybe_send_reply(ChPid, OkMsg),
    %% A prefetch count of 0 means no limitation, let's make it into something large for ra
    Prefetch = case ConsumerPrefetchCount of
                   0 -> 2000;
                   Other -> Other
               end,
    {ok, FState} = ra_fifo_client:checkout(quorum_ctag(ConsumerTag), Prefetch, FState0),
    %% TODO maybe needs to be handled by ra? how can we manage the consumer deleted?
    rabbit_core_metrics:consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                         not NoAck, QName, ConsumerPrefetchCount, Args),
    {ok, FState}.

stateless_deliver({Name, _} = Pid, Delivery) ->
    ok = ra_fifo_client:untracked_enqueue(Name, [Pid],
                                          Delivery#delivery.message).

deliver(false, Delivery, FState0) ->
    {ok, FState} = ra_fifo_client:enqueue(Delivery#delivery.message, FState0),
    FState;
deliver(true, Delivery, FState0) ->
    {ok, FState} = ra_fifo_client:enqueue(Delivery#delivery.msg_seq_no,
                                          Delivery#delivery.message, FState0),
    FState.

info(Q) ->
    info(Q, [name, durable, auto_delete, arguments, pid, state, messages,
             messages_ready, messages_unacknowledged]).

infos(QName) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    info(Q, ?STATISTICS_KEYS).

info(Q, Items) ->
    [{Item, i(Item, Q)} || Item <- Items].

stat(_Q) ->
    {ok, 0, 0}.  %% TODO length, consumers count

%%----------------------------------------------------------------------------
dlx_mfa(#amqqueue{name = Resource} = Q) ->
    #resource{virtual_host = VHost} = Resource,
    DLX = init_dlx(args_policy_lookup(<<"dead-letter-exchange">>, fun res_arg/2, Q), Q),
    DLXRKey = args_policy_lookup(<<"dead-letter-routing-key">>, fun res_arg/2, Q),
    {?MODULE, dead_letter_publish, [VHost, DLX, DLXRKey, Q#amqqueue.name]}.
        
init_dlx(undefined, _Q) ->
    undefined;
init_dlx(DLX, #amqqueue{name = QName}) ->
    rabbit_misc:r(QName, exchange, DLX).

res_arg(_PolVal, ArgVal) -> ArgVal.

args_policy_lookup(Name, Resolve, Q = #amqqueue{arguments = Args}) ->
    AName = <<"x-", Name/binary>>,
    case {rabbit_policy:get(Name, Q), rabbit_misc:table_lookup(Args, AName)} of
        {undefined, undefined}       -> undefined;
        {undefined, {_Type, Val}}    -> Val;
        {Val,       undefined}       -> Val;
        {PolVal,    {_Type, ArgVal}} -> Resolve(PolVal, ArgVal)
    end.

dead_letter_publish(_, undefined, _, _, _) ->
    ok;
dead_letter_publish(VHost, X, RK, QName, ReasonMsgs) ->
    rabbit_vhost_dead_letter:publish(VHost, X, RK, QName, ReasonMsgs).

%% TODO escape hack
qname_to_rname(#resource{virtual_host = <<"/">>, name = Name}) ->
    erlang:binary_to_atom(<<"%2F_", Name/binary>>, utf8);
qname_to_rname(#resource{virtual_host = VHost, name = Name}) ->
    erlang:binary_to_atom(<<VHost/binary, "_", Name/binary>>, utf8).

find_quorum_queues(VHost) ->
    Node = node(),
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q = #amqqueue{vhost = VH,
                                              pid  = Pid,
                                              type = quorum}
                                    <- mnesia:table(rabbit_durable_queue),
                                VH =:= VHost,
                                qnode(Pid) == Node]))
      end).

i(name,               #amqqueue{name               = Name}) -> Name;
i(durable,            #amqqueue{durable            = Dur}) -> Dur;
i(auto_delete,        #amqqueue{auto_delete        = AD}) -> AD;
i(arguments,          #amqqueue{arguments          = Args}) -> Args;
i(pid,                #amqqueue{pid                = {Name, _}}) -> whereis(Name);
i(messages,           #amqqueue{pid                = {Name, _}}) ->
    quorum_messages(Name);
i(messages_ready,     #amqqueue{pid                = {Name, _}}) ->
    [{_, Enqueue, Checkout, _, Return}] = ets:lookup(ra_fifo_metrics, Name),
    Enqueue - Checkout + Return;
i(messages_unacknowledged, #amqqueue{pid           = {Name, _}}) ->
    [{_, _, Checkout, Settle, Return}] = ets:lookup(ra_fifo_metrics, Name),
    Checkout - Settle - Return;
i(policy, Q) ->
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(operator_policy, Q) ->
    case rabbit_policy:name_op(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(effective_policy_definition, Q) ->
    case rabbit_policy:effective_definition(Q) of
        undefined -> [];
        Def       -> Def
    end;
i(exclusive_consumer_pid, _Q) ->
    %% TODO
    '';
i(exclusive_consumer_tag, _Q) ->
    %% TODO
    '';
i(consumers, _Q) ->
    %% TODO
    0;
i(consumer_utilisation, _Q) ->
    %% TODO!
    0;
i(memory, #amqqueue{pid = {Name, _}}) ->
    try
        {memory, M} = process_info(whereis(Name), memory),
        M
    catch
        error:badarg ->
            0
    end;
i(state, #amqqueue{pid = {Name, _}}) ->
    %% TODO guess this is it by now?
    case whereis(Name) of
        undefined -> down;
        _ -> running
    end;
i(garbage_collection, #amqqueue{pid = {Name, _}}) ->
    try
        rabbit_misc:get_gc_info(whereis(Name))
    catch
        error:badarg ->
            []
    end;
i(_K, _Q) -> ''.

quorum_messages(Name) ->
    [{_, Enqueue, _, Settle, _}] = ets:lookup(ra_fifo_metrics, Name),
    Enqueue - Settle.

quorum_ctag(Int) when is_integer(Int) ->
    integer_to_binary(Int);
quorum_ctag(Other) ->
    Other.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

qnode(QPid) when is_pid(QPid) ->
    node(QPid);
qnode({_, Node}) ->
    Node.

check_invalid_arguments(QueueName, Args) ->
    Keys = [<<"x-expires">>, <<"x-message-ttl">>, <<"x-max-length">>,
            <<"x-max-length-bytes">>, <<"x-max-priority">>, <<"x-overflow">>,
            <<"x-queue-mode">>],
    [case rabbit_misc:table_lookup(Args, Key) of
         undefined -> ok;
         _TypeVal   -> rabbit_misc:protocol_error(
                         precondition_failed,
                         "invalid arg '~s' for ~s",
                         [Key, rabbit_misc:rs(QueueName)])
     end || Key <- Keys],
    ok.

queue_name(Name) ->
    case ets:lookup(quorum_mapping, Name) of
        [{_, QName}] ->
            QName;
        _ ->
            case mnesia:async_dirty(
                   fun () ->
                           qlc:e(qlc:q([QName || Q = #amqqueue{name = QName,
                                                               pid  = {N, _},
                                                               type = quorum}
                                                     <- mnesia:table(rabbit_durable_queue),
                                                 N == Name]))
                   end) of
                [QName] ->
                    %% TODO if not present, undefined!
                    ets:insert_new(quorum_mapping, {Name, QName}),
                    QName;
                [] ->
                    undefined
            end
    end.
