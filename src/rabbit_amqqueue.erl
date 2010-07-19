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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_amqqueue).

-export([start/0, declare/5, delete/3, purge/1]).
-export([internal_declare/2, internal_delete/1,
         maybe_run_queue_via_backing_queue/2,
         update_ram_duration/1, set_ram_duration_target/2,
         set_maximum_since_use/2]).
-export([pseudo_queue/2]).
-export([lookup/1, with/2, with_or_die/2, assert_equivalence/5,
         check_exclusive_access/2, with_exclusive_access_or_die/3,
         stat/1, deliver/2, requeue/3, ack/4]).
-export([list/1, info_keys/0, info/1, info/2, info_all/1, info_all/2]).
-export([consumers/1, consumers_all/1]).
-export([basic_get/3, basic_consume/7, basic_cancel/4]).
-export([notify_sent/2, unblock/2, flush_all/2]).
-export([commit_all/3, rollback_all/3, notify_down_all/2, limit_all/3]).
-export([on_node_down/1]).

-import(mnesia).
-import(gen_server2).
-import(lists).
-import(queue).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([name/0, qmsg/0]).

-type(name() :: rabbit_types:r('queue')).

-type(qlen() :: rabbit_types:ok(non_neg_integer())).
-type(qfun(A) :: fun ((rabbit_types:amqqueue()) -> A)).
-type(qmsg() :: {name(), pid(), msg_id(), boolean(), rabbit_types:message()}).
-type(msg_id() :: non_neg_integer()).
-type(ok_or_errors() ::
      'ok' | {'error', [{'error' | 'exit' | 'throw', any()}]}).

-spec(start/0 :: () -> 'ok').
-spec(declare/5 ::
        (name(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid()))
        -> {'new' | 'existing', rabbit_types:amqqueue()}).
-spec(lookup/1 ::
        (name()) -> rabbit_types:ok(rabbit_types:amqqueue()) |
                    rabbit_types:error('not_found')).
-spec(with/2 :: (name(), qfun(A)) -> A | rabbit_types:error('not_found')).
-spec(with_or_die/2 :: (name(), qfun(A)) -> A).
-spec(assert_equivalence/5 ::
        (rabbit_types:amqqueue(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid))
        -> ok).
-spec(check_exclusive_access/2 :: (rabbit_types:amqqueue(), pid()) -> 'ok').
-spec(with_exclusive_access_or_die/3 :: (name(), pid(), qfun(A)) -> A).
-spec(list/1 :: (rabbit_types:vhost()) -> [rabbit_types:amqqueue()]).
-spec(info_keys/0 :: () -> [rabbit_types:info_key()]).
-spec(info/1 :: (rabbit_types:amqqueue()) -> [rabbit_types:info()]).
-spec(info/2 ::
        (rabbit_types:amqqueue(), [rabbit_types:info_key()])
        -> [rabbit_types:info()]).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [[rabbit_types:info()]]).
-spec(info_all/2 :: (rabbit_types:vhost(), [rabbit_types:info_key()])
                    -> [[rabbit_types:info()]]).
-spec(consumers/1 ::
        (rabbit_types:amqqueue())
        -> [{pid(), rabbit_types:ctag(), boolean()}]).
-spec(consumers_all/1 ::
        (rabbit_types:vhost())
        -> [{name(), pid(), rabbit_types:ctag(), boolean()}]).
-spec(stat/1 ::
        (rabbit_types:amqqueue())
        -> {'ok', non_neg_integer(), non_neg_integer()}).
-spec(delete/3 ::
      (rabbit_types:amqqueue(), 'false', 'false')
        -> qlen();
      (rabbit_types:amqqueue(), 'true' , 'false')
        -> qlen() | rabbit_types:error('in_use');
      (rabbit_types:amqqueue(), 'false', 'true' )
        -> qlen() | rabbit_types:error('not_empty');
      (rabbit_types:amqqueue(), 'true' , 'true' )
        -> qlen() |
           rabbit_types:error('in_use') |
           rabbit_types:error('not_empty')).
-spec(purge/1 :: (rabbit_types:amqqueue()) -> qlen()).
-spec(deliver/2 :: (pid(), rabbit_types:delivery()) -> boolean()).
-spec(requeue/3 :: (pid(), [msg_id()],  pid()) -> 'ok').
-spec(ack/4 ::
        (pid(), rabbit_types:maybe(rabbit_types:txn()), [msg_id()], pid())
        -> 'ok').
-spec(commit_all/3 :: ([pid()], rabbit_types:txn(), pid()) -> ok_or_errors()).
-spec(rollback_all/3 :: ([pid()], rabbit_types:txn(), pid()) -> 'ok').
-spec(notify_down_all/2 :: ([pid()], pid()) -> ok_or_errors()).
-spec(limit_all/3 :: ([pid()], pid(), pid() | 'undefined') -> ok_or_errors()).
-spec(basic_get/3 :: (rabbit_types:amqqueue(), pid(), boolean()) ->
             {'ok', non_neg_integer(), qmsg()} | 'empty').
-spec(basic_consume/7 ::
      (rabbit_types:amqqueue(), boolean(), pid(), pid() | 'undefined',
       rabbit_types:ctag(), boolean(), any())
        -> rabbit_types:ok_or_error('exclusive_consume_unavailable')).
-spec(basic_cancel/4 ::
        (rabbit_types:amqqueue(), pid(), rabbit_types:ctag(), any()) -> 'ok').
-spec(notify_sent/2 :: (pid(), pid()) -> 'ok').
-spec(unblock/2 :: (pid(), pid()) -> 'ok').
-spec(flush_all/2 :: ([pid()], pid()) -> 'ok').
-spec(internal_declare/2 ::
        (rabbit_types:amqqueue(), boolean())
        -> rabbit_types:amqqueue() | 'not_found').
-spec(internal_delete/1 :: (name()) -> rabbit_types:ok_or_error('not_found')).
-spec(maybe_run_queue_via_backing_queue/2 ::
        (pid(), (fun ((A) -> A))) -> 'ok').
-spec(update_ram_duration/1 :: (pid()) -> 'ok').
-spec(set_ram_duration_target/2 :: (pid(), number() | 'infinity') -> 'ok').
-spec(set_maximum_since_use/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').
-spec(pseudo_queue/2 :: (binary(), pid()) -> rabbit_types:amqqueue()).

-endif.

%%----------------------------------------------------------------------------

start() ->
    DurableQueues = find_durable_queues(),
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    ok = BQ:start([QName || #amqqueue{name = QName} <- DurableQueues]),
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {rabbit_amqqueue_sup,
                {rabbit_amqqueue_sup, start_link, []},
                transient, infinity, supervisor, [rabbit_amqqueue_sup]}),
    _RealDurableQueues = recover_durable_queues(DurableQueues),
    ok.

find_durable_queues() ->
    Node = node(),
    %% TODO: use dirty ops instead
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              qlc:e(qlc:q([Q || Q = #amqqueue{pid = Pid}
                                    <- mnesia:table(rabbit_durable_queue),
                                node(Pid) == Node]))
      end).

recover_durable_queues(DurableQueues) ->
    Qs = [start_queue_process(Q) || Q <- DurableQueues],
    [Q || Q <- Qs, gen_server2:call(Q#amqqueue.pid, {init, true}) == Q].

declare(QueueName, Durable, AutoDelete, Args, Owner) ->
    Q = start_queue_process(#amqqueue{name = QueueName,
                                      durable = Durable,
                                      auto_delete = AutoDelete,
                                      arguments = Args,
                                      exclusive_owner = Owner,
                                      pid = none}),
    case gen_server2:call(Q#amqqueue.pid, {init, false}) of
        not_found -> rabbit_misc:not_found(QueueName);
        Q1        -> Q1
    end.

internal_declare(Q = #amqqueue{name = QueueName}, Recover) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case Recover of
                  true ->
                      ok = store_queue(Q),
                      Q;
                  false ->
                      case mnesia:wread({rabbit_queue, QueueName}) of
                          [] ->
                              case mnesia:read({rabbit_durable_queue,
                                                QueueName}) of
                                  []  -> ok = store_queue(Q),
                                         ok = add_default_binding(Q),
                                         Q;
                                  [_] -> not_found %% Q exists on stopped node
                              end;
                          [ExistingQ] ->
                              ExistingQ
                      end
              end
      end).

store_queue(Q = #amqqueue{durable = true}) ->
    ok = mnesia:write(rabbit_durable_queue, Q, write),
    ok = mnesia:write(rabbit_queue, Q, write),
    ok;
store_queue(Q = #amqqueue{durable = false}) ->
    ok = mnesia:write(rabbit_queue, Q, write),
    ok.

start_queue_process(Q) ->
    {ok, Pid} = rabbit_amqqueue_sup:start_child([Q]),
    Q#amqqueue{pid = Pid}.

add_default_binding(#amqqueue{name = QueueName}) ->
    Exchange = rabbit_misc:r(QueueName, exchange, <<>>),
    RoutingKey = QueueName#resource.name,
    rabbit_exchange:add_binding(Exchange, QueueName, RoutingKey, [],
                                fun (_X, _Q) -> ok end),
    ok.

lookup(Name) ->
    rabbit_misc:dirty_read({rabbit_queue, Name}).

with(Name, F, E) ->
    case lookup(Name) of
        {ok, Q} -> rabbit_misc:with_exit_handler(E, fun () -> F(Q) end);
        {error, not_found} -> E()
    end.

with(Name, F) ->
    with(Name, F, fun () -> {error, not_found} end).
with_or_die(Name, F) ->
    with(Name, F, fun () -> rabbit_misc:not_found(Name) end).

assert_equivalence(#amqqueue{durable = Durable, auto_delete = AutoDelete} = Q,
                   Durable, AutoDelete, _Args, Owner) ->
    check_exclusive_access(Q, Owner, strict);
assert_equivalence(#amqqueue{name = QueueName},
                   _Durable, _AutoDelete, _Args, _Owner) ->
    rabbit_misc:protocol_error(
      not_allowed, "parameters for ~s not equivalent",
      [rabbit_misc:rs(QueueName)]).

check_exclusive_access(Q, Owner) -> check_exclusive_access(Q, Owner, lax).

check_exclusive_access(#amqqueue{exclusive_owner = Owner}, Owner, _MatchType) ->
    ok;
check_exclusive_access(#amqqueue{exclusive_owner = none}, _ReaderPid, lax) ->
    ok;
check_exclusive_access(#amqqueue{name = QueueName}, _ReaderPid, _MatchType) ->
    rabbit_misc:protocol_error(
      resource_locked,
      "cannot obtain exclusive access to locked ~s",
      [rabbit_misc:rs(QueueName)]).

with_exclusive_access_or_die(Name, ReaderPid, F) ->
    with_or_die(Name,
                fun (Q) -> check_exclusive_access(Q, ReaderPid), F(Q) end).

list(VHostPath) ->
    mnesia:dirty_match_object(
      rabbit_queue,
      #amqqueue{name = rabbit_misc:r(VHostPath, queue), _ = '_'}).

info_keys() -> rabbit_amqqueue_process:info_keys().

map(VHostPath, F) -> rabbit_misc:filter_exit_map(F, list(VHostPath)).

info(#amqqueue{ pid = QPid }) ->
    delegate_pcall(QPid, 9, info, infinity).

info(#amqqueue{ pid = QPid }, Items) ->
    case delegate_pcall(QPid, 9, {info, Items}, infinity) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

info_all(VHostPath) -> map(VHostPath, fun (Q) -> info(Q) end).

info_all(VHostPath, Items) -> map(VHostPath, fun (Q) -> info(Q, Items) end).

consumers(#amqqueue{ pid = QPid }) ->
    delegate_pcall(QPid, 9, consumers, infinity).

consumers_all(VHostPath) ->
    lists:concat(
      map(VHostPath,
          fun (Q) -> [{Q#amqqueue.name, ChPid, ConsumerTag, AckRequired} ||
                         {ChPid, ConsumerTag, AckRequired} <- consumers(Q)]
          end)).

stat(#amqqueue{pid = QPid}) -> delegate_call(QPid, stat, infinity).

delete(#amqqueue{ pid = QPid }, IfUnused, IfEmpty) ->
    delegate_call(QPid, {delete, IfUnused, IfEmpty}, infinity).

purge(#amqqueue{ pid = QPid }) -> delegate_call(QPid, purge, infinity).

deliver(QPid, #delivery{immediate = true,
                        txn = Txn, sender = ChPid, message = Message}) ->
    gen_server2:call(QPid, {deliver_immediately, Txn, Message, ChPid},
                     infinity);
deliver(QPid, #delivery{mandatory = true,
                        txn = Txn, sender = ChPid, message = Message}) ->
    gen_server2:call(QPid, {deliver, Txn, Message, ChPid}, infinity),
    true;
deliver(QPid, #delivery{txn = Txn, sender = ChPid, message = Message}) ->
    gen_server2:cast(QPid, {deliver, Txn, Message, ChPid}),
    true.

requeue(QPid, MsgIds, ChPid) ->
    delegate_call(QPid, {requeue, MsgIds, ChPid}, infinity).

ack(QPid, Txn, MsgIds, ChPid) ->
    delegate_pcast(QPid, 7, {ack, Txn, MsgIds, ChPid}).

commit_all(QPids, Txn, ChPid) ->
    safe_delegate_call_ok(
      fun (QPid) -> exit({queue_disappeared, QPid}) end,
      fun (QPid) -> gen_server2:call(QPid, {commit, Txn, ChPid}, infinity) end,
      QPids).

rollback_all(QPids, Txn, ChPid) ->
    delegate:invoke_no_result(
      QPids, fun (QPid) -> gen_server2:cast(QPid, {rollback, Txn, ChPid}) end).

notify_down_all(QPids, ChPid) ->
    safe_delegate_call_ok(
      %% we don't care if the queue process has terminated in the
      %% meantime
      fun (_)    -> ok end,
      fun (QPid) -> gen_server2:call(QPid, {notify_down, ChPid}, infinity) end,
      QPids).

limit_all(QPids, ChPid, LimiterPid) ->
    delegate:invoke_no_result(
      QPids, fun (QPid) ->
                     gen_server2:cast(QPid, {limit, ChPid, LimiterPid})
             end).

basic_get(#amqqueue{pid = QPid}, ChPid, NoAck) ->
    delegate_call(QPid, {basic_get, ChPid, NoAck}, infinity).

basic_consume(#amqqueue{pid = QPid}, NoAck, ChPid, LimiterPid,
              ConsumerTag, ExclusiveConsume, OkMsg) ->
    delegate_call(QPid, {basic_consume, NoAck, ChPid,
                         LimiterPid, ConsumerTag, ExclusiveConsume, OkMsg},
                  infinity).

basic_cancel(#amqqueue{pid = QPid}, ChPid, ConsumerTag, OkMsg) ->
    ok = delegate_call(QPid, {basic_cancel, ChPid, ConsumerTag, OkMsg},
                       infinity).

notify_sent(QPid, ChPid) ->
    delegate_pcast(QPid, 7, {notify_sent, ChPid}).

unblock(QPid, ChPid) ->
    delegate_pcast(QPid, 7, {unblock, ChPid}).

flush_all(QPids, ChPid) ->
    delegate:invoke_no_result(
      QPids, fun (QPid) -> gen_server2:cast(QPid, {flush, ChPid}) end).

internal_delete1(QueueName) ->
    ok = mnesia:delete({rabbit_queue, QueueName}),
    ok = mnesia:delete({rabbit_durable_queue, QueueName}),
    %% we want to execute some things, as
    %% decided by rabbit_exchange, after the
    %% transaction.
    rabbit_exchange:delete_queue_bindings(QueueName).

internal_delete(QueueName) ->
    case
        rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_queue, QueueName}) of
                      []  -> {error, not_found};
                      [_] -> internal_delete1(QueueName)
                  end
          end) of
        Err = {error, _} -> Err;
        PostHook ->
            PostHook(),
            ok
    end.

maybe_run_queue_via_backing_queue(QPid, Fun) ->
    gen_server2:pcall(QPid, 7, {maybe_run_queue_via_backing_queue, Fun},
                      infinity).

update_ram_duration(QPid) ->
    gen_server2:pcast(QPid, 8, update_ram_duration).

set_ram_duration_target(QPid, Duration) ->
    gen_server2:pcast(QPid, 8, {set_ram_duration_target, Duration}).

set_maximum_since_use(QPid, Age) ->
    gen_server2:pcast(QPid, 8, {set_maximum_since_use, Age}).

on_node_down(Node) ->
    [Hook() ||
        Hook <- rabbit_misc:execute_mnesia_transaction(
                  fun () ->
                          qlc:e(qlc:q([delete_queue(QueueName) ||
                                          #amqqueue{name = QueueName, pid = Pid}
                                              <- mnesia:table(rabbit_queue),
                                          node(Pid) == Node]))
                  end)],
    ok.

delete_queue(QueueName) ->
    Post = rabbit_exchange:delete_transient_queue_bindings(QueueName),
    ok = mnesia:delete({rabbit_queue, QueueName}),
    Post.

pseudo_queue(QueueName, Pid) ->
    #amqqueue{name = QueueName,
              durable = false,
              auto_delete = false,
              arguments = [],
              pid = Pid}.

safe_delegate_call_ok(H, F, Pids) ->
    {_, Bad} = delegate:invoke(Pids,
                               fun (Pid) ->
                                       rabbit_misc:with_exit_handler(
                                         fun () -> H(Pid) end,
                                         fun () -> F(Pid) end)
                               end),
    case Bad of
        [] -> ok;
        _  -> {error, Bad}
    end.

delegate_call(Pid, Msg, Timeout) ->
    delegate:invoke(Pid, fun (P) -> gen_server2:call(P, Msg, Timeout) end).

delegate_pcall(Pid, Pri, Msg, Timeout) ->
    delegate:invoke(Pid,
                    fun (P) -> gen_server2:pcall(P, Pri, Msg, Timeout) end).

delegate_pcast(Pid, Pri, Msg) ->
    delegate:invoke_no_result(Pid,
                              fun (P) -> gen_server2:pcast(P, Pri, Msg) end).

