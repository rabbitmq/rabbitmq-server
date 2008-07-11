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

-export([start/0, recover/0, declare/5, delete/3, purge/1, internal_delete/1]).
-export([pseudo_queue/3]).
-export([lookup/1, with/2, with_or_die/2, list_vhost_queues/1,
         stat/1, stat_all/0, deliver/5, redeliver/2, requeue/3, ack/4,
         commit/2, rollback/2]).
-export([add_binding/4, delete_binding/4, binding_forcibly_removed/2]).
-export([claim_queue/2]).
-export([basic_get/3, basic_consume/7, basic_cancel/4]).
-export([notify_sent/2, notify_down/2]).
-export([on_node_down/1]).

-import(mnesia).
-import(gen_server).
-import(lists).
-import(queue).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(qstats() :: {'ok', queue_name(), non_neg_integer(), non_neg_integer()}).
-type(qlen() :: {'ok', non_neg_integer()}).
-type(qfun(A) :: fun ((amqqueue()) -> A)).
-type(bind_res() :: {'ok', non_neg_integer()} |
      {'error', 'queue_not_found' | 'exchange_not_found'}).
-spec(start/0 :: () -> 'ok').
-spec(recover/0 :: () -> 'ok').
-spec(declare/5 :: (realm_name(), name(), bool(), bool(), amqp_table()) ->
             amqqueue()).
-spec(add_binding/4 ::
      (queue_name(), exchange_name(), routing_key(), amqp_table()) ->
            bind_res() | {'error', 'durability_settings_incompatible'}).
-spec(delete_binding/4 ::
      (queue_name(), exchange_name(), routing_key(), amqp_table()) ->
             bind_res() | {'error', 'binding_not_found'}).
-spec(lookup/1 :: (queue_name()) -> {'ok', amqqueue()} | not_found()).
-spec(with/2 :: (queue_name(), qfun(A)) -> A | not_found()).
-spec(with_or_die/2 :: (queue_name(), qfun(A)) -> A).
-spec(list_vhost_queues/1 :: (vhost()) -> [amqqueue()]).
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
-spec(commit/2 :: (pid(), txn()) -> 'ok').
-spec(rollback/2 :: (pid(), txn()) -> 'ok').
-spec(notify_down/2 :: (amqqueue(), pid()) -> 'ok').
-spec(binding_forcibly_removed/2 :: (binding_spec(), queue_name()) -> 'ok').
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
-spec(pseudo_queue/3 :: (realm_name(), binary(), pid()) -> amqqueue()).

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
              lists:foreach(fun recover_queue/1, Queues),
              ok
      end).

declare(RealmName, NameBin, Durable, AutoDelete, Args) ->
    QName = rabbit_misc:r(RealmName, queue, NameBin),
    Q = start_queue_process(#amqqueue{name = QName,
                                      durable = Durable,
                                      auto_delete = AutoDelete,
                                      arguments = Args,
                                      binding_specs = [],
                                      pid = none}),
    case rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   case mnesia:wread({amqqueue, QName}) of
                       [] -> ok = recover_queue(Q),
                             ok = rabbit_realm:add(RealmName, QName),
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

recover_queue(Q) ->
    ok = store_queue(Q),
    ok = recover_bindings(Q),
    ok.

default_binding_spec(#resource{virtual_host = VHost, name = Name}) ->
    #binding_spec{exchange_name = rabbit_misc:r(VHost, exchange, <<>>),
                  routing_key = Name,
                  arguments = []}.

recover_bindings(Q = #amqqueue{name = QueueName, binding_specs = Specs}) ->
    ok = rabbit_exchange:add_binding(default_binding_spec(QueueName), Q),
    lists:foreach(fun (B) ->
                          ok = rabbit_exchange:add_binding(B, Q)
                  end, Specs),
    ok.

modify_bindings(QueueName, ExchangeName, RoutingKey, Arguments,
                SpecPresentFun, SpecAbsentFun) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({amqqueue, QueueName}) of
                  [Q = #amqqueue{binding_specs = Specs0}] ->
                      Spec = #binding_spec{exchange_name = ExchangeName,
                                           routing_key = RoutingKey,
                                           arguments = Arguments},
                      case (case lists:member(Spec, Specs0) of
                                true  -> SpecPresentFun;
                                false -> SpecAbsentFun
                            end)(Q, Spec) of
                          {ok, #amqqueue{binding_specs = Specs}} ->
                              {ok, length(Specs)};
                          {error, not_found} ->
                              {error, exchange_not_found};
                          Other -> Other
                      end;
                  [] -> {error, queue_not_found}
              end
      end).

update_bindings(Q = #amqqueue{binding_specs = Specs0}, Spec,
                UpdateSpecFun, UpdateExchangeFun) ->
    Q1 = Q#amqqueue{binding_specs = UpdateSpecFun(Spec, Specs0)},
    case UpdateExchangeFun(Spec, Q1) of
        ok    -> store_queue(Q1),
                 {ok, Q1};
        Other -> Other
    end.

add_binding(QueueName, ExchangeName, RoutingKey, Arguments) ->
    modify_bindings(
      QueueName, ExchangeName, RoutingKey, Arguments,
      fun (Q, _Spec) -> {ok, Q} end,
      fun (Q, Spec) -> update_bindings(
                         Q, Spec,
                         fun (S, Specs) -> [S | Specs] end,
                         fun rabbit_exchange:add_binding/2)
      end).

delete_binding(QueueName, ExchangeName, RoutingKey, Arguments) ->
    modify_bindings(
      QueueName, ExchangeName, RoutingKey, Arguments,
      fun (Q, Spec) -> update_bindings(
                         Q, Spec,
                         fun lists:delete/2,
                         fun rabbit_exchange:delete_binding/2)
      end,
      fun (Q, Spec) ->
              %% the following is essentially a no-op, though crucially
              %% it produces {error, not_found} when the exchange does
              %% not exist.
              case rabbit_exchange:delete_binding(Spec, Q) of
                  ok    -> {error, binding_not_found};
                  Other -> Other
              end
      end).

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

list_vhost_queues(VHostPath) ->
    mnesia:dirty_match_object(
      #amqqueue{name = rabbit_misc:r(VHostPath, queue), _ = '_'}).

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

commit(QPid, Txn) ->
    gen_server:call(QPid, {commit, Txn}).

rollback(QPid, Txn) ->
    gen_server:cast(QPid, {rollback, Txn}).

notify_down(#amqqueue{ pid = QPid }, ChPid) ->
    gen_server:call(QPid, {notify_down, ChPid}).

binding_forcibly_removed(BindingSpec, QueueName) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({amqqueue, QueueName}) of
                  [] -> ok;
                  [Q = #amqqueue{binding_specs = Specs}] ->
                      store_queue(Q#amqqueue{binding_specs =
                                             lists:delete(BindingSpec, Specs)})
              end
      end).

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

delete_bindings(Q = #amqqueue{binding_specs = Specs}) ->
    lists:foreach(fun (BindingSpec) ->
                          ok = rabbit_exchange:delete_binding(
                                 BindingSpec, Q)
                  end, Specs).

internal_delete(QueueName) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({amqqueue, QueueName}) of
                  [] -> {error, not_found};
                  [Q] ->
                      ok = delete_temp(Q),
                      ok = mnesia:delete({durable_queues, QueueName}),
                      ok = rabbit_realm:delete_from_all(QueueName),
                      ok
              end
      end).

delete_temp(Q = #amqqueue{name = QueueName}) ->
    ok = delete_bindings(Q),
    ok = rabbit_exchange:delete_binding(
           default_binding_spec(QueueName), Q),
    ok = mnesia:delete({amqqueue, QueueName}),
    ok.

delete_queue(Q = #amqqueue{name = QueueName, durable = Durable}) ->
    ok = delete_temp(Q),
    if
        Durable -> ok;
        true -> ok = rabbit_realm:delete_from_all(QueueName)
    end.

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

pseudo_queue(RealmName, NameBin, Pid) ->
    #amqqueue{name = rabbit_misc:r(RealmName, queue, NameBin),
              durable = false,
              auto_delete = false,
              arguments = [],
              binding_specs = [],
              pid = Pid}.
