%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_amqqueue).

-export([start/0, recover/0, declare/4, delete/3, purge/1, internal_delete/1]).
-export([pseudo_queue/2]).
-export([lookup/1, with/2, with_or_die/2, list/0, list_vhost_queues/1,
         stat/1, stat_all/0, deliver/5, redeliver/2, requeue/3, ack/4]).
-export([info/1, info/2]).
-export([claim_queue/2]).
-export([basic_get/3, basic_consume/7, basic_cancel/4]).
-export([notify_sent/2]).
-export([commit_all/2, rollback_all/2, notify_down_all/2]).
-export([on_node_down/1]).

-import(mnesia).
-import(gen_server).
-import(lists).
-import(queue).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(CALL_TIMEOUT, 5000).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(qstats() :: {'ok', queue_name(), non_neg_integer(), non_neg_integer()}).
-type(qlen() :: {'ok', non_neg_integer()}).
-type(qfun(A) :: fun ((amqqueue()) -> A)).
-type(ok_or_errors() ::
      'ok' | {'error', [{'error' | 'exit' | 'throw', any()}]}).
-type(info_key() :: atom()).
-type(info() :: {info_key(), any()}).

-spec(start/0 :: () -> 'ok').
-spec(recover/0 :: () -> 'ok').
-spec(declare/4 :: (queue_name(), bool(), bool(), amqp_table()) ->
             amqqueue()).
-spec(lookup/1 :: (queue_name()) -> {'ok', amqqueue()} | not_found()).
-spec(with/2 :: (queue_name(), qfun(A)) -> A | not_found()).
-spec(with_or_die/2 :: (queue_name(), qfun(A)) -> A).
-spec(list/0 :: () -> [amqqueue()]).
-spec(list_vhost_queues/1 :: (vhost()) -> [amqqueue()]).
-spec(info/1 :: (amqqueue()) -> [info()]).
-spec(info/2 ::
      (amqqueue(), info_key()) -> info();
      (amqqueue(), [info_key()]) -> [info()]).
-spec(stat/1 :: (amqqueue()) -> qstats()).
-spec(stat_all/0 :: () -> [qstats()]).
-spec(delete/3 ::
      (amqqueue(), 'false', 'false') -> qlen();
      (amqqueue(), 'true' , 'false') -> qlen() | {'error', 'in_use'};
      (amqqueue(), 'false', 'true' ) -> qlen() | {'error', 'not_empty'};
      (amqqueue(), 'true' , 'true' ) -> qlen() |
                                            {'error', 'in_use'} |
                                            {'error', 'not_empty'}).
-spec(purge/1 :: (amqqueue()) -> qlen()).
-spec(deliver/5 :: (bool(), bool(), maybe(txn()), message(), pid()) -> bool()).
-spec(redeliver/2 :: (pid(), [{message(), bool()}]) -> 'ok').
-spec(requeue/3 :: (pid(), [msg_id()],  pid()) -> 'ok').
-spec(ack/4 :: (pid(), maybe(txn()), [msg_id()], pid()) -> 'ok').
-spec(commit_all/2 :: ([pid()], txn()) -> ok_or_errors()).
-spec(rollback_all/2 :: ([pid()], txn()) -> ok_or_errors()).
-spec(notify_down_all/2 :: ([pid()], pid()) -> ok_or_errors()).
-spec(claim_queue/2 :: (amqqueue(), pid()) -> 'ok' | 'locked').
-spec(basic_get/3 :: (amqqueue(), pid(), bool()) ->
             {'ok', non_neg_integer(), msg()} | 'empty').
-spec(basic_consume/7 ::
      (amqqueue(), bool(), pid(), pid(), ctag(), bool(), any()) ->
             'ok' | {'error', 'queue_owned_by_another_connection' |
                     'exclusive_consume_unavailable'}).
-spec(basic_cancel/4 :: (amqqueue(), pid(), ctag(), any()) -> 'ok').
-spec(notify_sent/2 :: (pid(), pid()) -> 'ok').
-spec(internal_delete/1 :: (queue_name()) -> 'ok' | not_found()).
-spec(on_node_down/1 :: (node()) -> 'ok').
-spec(pseudo_queue/2 :: (binary(), pid()) -> amqqueue()).

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {rabbit_amqqueue_sup,
                {rabbit_amqqueue_sup, start_link, []},
                transient, infinity, supervisor, [rabbit_amqqueue_sup]}),
    ok.

recover() ->
    ok = recover_durable_queues(),
    ok.

recover_durable_queues() ->
    Node = node(),
    %% TODO: use dirty ops instead
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  qlc:e(qlc:q([Q || Q = #amqqueue{pid = Pid}
                                        <- mnesia:table(durable_queues),
                                    node(Pid) == Node]))
          end),
    Queues = lists:map(fun start_queue_process/1, R),
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              lists:foreach(fun store_queue/1, Queues),
              ok
      end).

declare(QueueName, Durable, AutoDelete, Args) ->
    Q = start_queue_process(#amqqueue{name = QueueName,
                                      durable = Durable,
                                      auto_delete = AutoDelete,
                                      arguments = Args,
                                      pid = none}),
    case rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   case mnesia:wread({amqqueue, QueueName}) of
                       [] -> ok = store_queue(Q),
                             ok = add_default_binding(Q),
                             Q;
                       [ExistingQ] -> ExistingQ
                   end
           end) of
        Q         -> Q;
        ExistingQ -> exit(Q#amqqueue.pid, shutdown),
                     ExistingQ
    end.

store_queue(Q = #amqqueue{durable = true}) ->
    ok = mnesia:write(durable_queues, Q, write),
    ok = mnesia:write(Q),
    ok;
store_queue(Q = #amqqueue{durable = false}) ->
    ok = mnesia:write(Q),
    ok.

start_queue_process(Q) ->
    {ok, Pid} = supervisor:start_child(rabbit_amqqueue_sup, [Q]),
    Q#amqqueue{pid = Pid}.

add_default_binding(#amqqueue{name = QueueName}) ->
    Exchange = rabbit_misc:r(QueueName, exchange, <<>>),
    RoutingKey = QueueName#resource.name,
    rabbit_exchange:add_binding(Exchange, QueueName, RoutingKey, []),
    ok.

lookup(Name) ->
    rabbit_misc:dirty_read({amqqueue, Name}).

with(Name, F, E) ->
    case lookup(Name) of
        {ok, Q} -> rabbit_misc:with_exit_handler(E, fun () -> F(Q) end);
        {error, not_found} -> E()
    end.

with(Name, F) ->
    with(Name, F, fun () -> {error, not_found} end).
with_or_die(Name, F) ->
    with(Name, F, fun () -> rabbit_misc:protocol_error(
                              not_found, "no ~s", [rabbit_misc:rs(Name)])
                  end).

list() -> rabbit_misc:dirty_read_all(amqqueue).

list_vhost_queues(VHostPath) ->
    mnesia:dirty_match_object(
      #amqqueue{name = rabbit_misc:r(VHostPath, queue), _ = '_'}).

info(#amqqueue{ pid = QPid }) ->
    gen_server:call(QPid, info).

info(#amqqueue{ pid = QPid }, ItemOrItems) ->
    case gen_server:call(QPid, {info, ItemOrItems}) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

stat(#amqqueue{pid = QPid}) -> gen_server:call(QPid, stat).

stat_all() ->
    lists:map(fun stat/1, rabbit_misc:dirty_read_all(amqqueue)).

delete(#amqqueue{ pid = QPid }, IfUnused, IfEmpty) ->
    gen_server:call(QPid, {delete, IfUnused, IfEmpty}).

purge(#amqqueue{ pid = QPid }) -> gen_server:call(QPid, purge).

deliver(_IsMandatory, true, Txn, Message, QPid) ->
    gen_server:call(QPid, {deliver_immediately, Txn, Message});
deliver(true, _IsImmediate, Txn, Message, QPid) ->
    gen_server:call(QPid, {deliver, Txn, Message}),
    true;
deliver(false, _IsImmediate, Txn, Message, QPid) ->
    gen_server:cast(QPid, {deliver, Txn, Message}),
    true.

redeliver(QPid, Messages) ->
    gen_server:cast(QPid, {redeliver, Messages}).

requeue(QPid, MsgIds, ChPid) ->
    gen_server:cast(QPid, {requeue, MsgIds, ChPid}).

ack(QPid, Txn, MsgIds, ChPid) ->
    gen_server:cast(QPid, {ack, Txn, MsgIds, ChPid}).

commit_all(QPids, Txn) ->
    Timeout = length(QPids) * ?CALL_TIMEOUT,
    safe_pmap_ok(
      fun (QPid) -> exit({queue_disappeared, QPid}) end,
      fun (QPid) -> gen_server:call(QPid, {commit, Txn}, Timeout) end,
      QPids).

rollback_all(QPids, Txn) ->
    safe_pmap_ok(
      fun (QPid) -> exit({queue_disappeared, QPid}) end,
      fun (QPid) -> gen_server:cast(QPid, {rollback, Txn}) end,
      QPids).

notify_down_all(QPids, ChPid) ->
    Timeout = length(QPids) * ?CALL_TIMEOUT,
    safe_pmap_ok(
      %% we don't care if the queue process has terminated in the
      %% meantime
      fun (_)    -> ok end,
      fun (QPid) -> gen_server:call(QPid, {notify_down, ChPid}, Timeout) end,
      QPids).

claim_queue(#amqqueue{pid = QPid}, ReaderPid) ->
    gen_server:call(QPid, {claim_queue, ReaderPid}).

basic_get(#amqqueue{pid = QPid}, ChPid, NoAck) ->
    gen_server:call(QPid, {basic_get, ChPid, NoAck}).

basic_consume(#amqqueue{pid = QPid}, NoAck, ReaderPid, ChPid,
              ConsumerTag, ExclusiveConsume, OkMsg) ->
    gen_server:call(QPid, {basic_consume, NoAck, ReaderPid, ChPid,
                           ConsumerTag, ExclusiveConsume, OkMsg}).

basic_cancel(#amqqueue{pid = QPid}, ChPid, ConsumerTag, OkMsg) ->
    ok = gen_server:call(QPid, {basic_cancel, ChPid, ConsumerTag, OkMsg}).

notify_sent(QPid, ChPid) ->
    gen_server:cast(QPid, {notify_sent, ChPid}).

internal_delete(QueueName) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({amqqueue, QueueName}) of
                  [] -> {error, not_found};
                  [Q] ->
                      ok = delete_queue(Q),
                      ok = mnesia:delete({durable_queues, QueueName}),
                      ok
              end
      end).

delete_queue(#amqqueue{name = QueueName}) ->
    ok = rabbit_exchange:delete_bindings_for_queue(QueueName),
    ok = mnesia:delete({amqqueue, QueueName}),
    ok.

on_node_down(Node) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              qlc:fold(
                fun (Q, Acc) -> ok = delete_queue(Q), Acc end,
                ok,
                qlc:q([Q || Q = #amqqueue{pid = Pid}
                                <- mnesia:table(amqqueue),
                            node(Pid) == Node]))
      end).

pseudo_queue(QueueName, Pid) ->
    #amqqueue{name = QueueName,
              durable = false,
              auto_delete = false,
              arguments = [],
              pid = Pid}.

safe_pmap_ok(H, F, L) ->
    case [R || R <- rabbit_misc:upmap(
                      fun (V) ->
                              try
                                  rabbit_misc:with_exit_handler(
                                    fun () -> H(V) end,
                                    fun () -> F(V) end)
                              catch Class:Reason -> {Class, Reason}
                              end
                      end, L),
               R =/= ok] of
        []     -> ok;
        Errors -> {error, Errors}
    end.
