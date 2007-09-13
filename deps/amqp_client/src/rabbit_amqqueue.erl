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
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_amqqueue).

-export([start_link/0]).
-export([recover/0, declare/5, delete/3, purge/1, purge_message_buffer/1, internal_delete/2]).
-export([pseudo_queue/3]).
-export([lookup/1, lookup_or_die/2, list_vhost_queues/1,
         stat/1, stat_all/0, deliver/4]).
-export([add_binding/4, binding_forcibly_removed/2]).
-export([claim_queue/2]).
-export([basic_get/1, basic_consume/7, basic_cancel/4]).
-export([notify_sent/1]).
-export([cleanup/1, on_node_down/1]).

-import(mnesia).
-import(gen_server).
-import(lists).
-import(queue).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

start_link() ->
    rabbit_monitor:start_link({local, rabbit_amqqueue_monitor},
                              {?MODULE, cleanup}).
    
recover() ->
    ok = recover_durable_queues(),
    ok.

recover_durable_queues() ->
    Node = node(),
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              qlc:e(qlc:q([Q || Q = #amqqueue{pid = Pid}
                                    <- mnesia:table(durable_queues),
                                node(Pid) == Node]))
      end,
      fun (Queues) ->
              Queues1 = lists:map(fun start_queue_process/1, Queues),
              rabbit_misc:execute_simple_mnesia_transaction(
                fun () ->
                        lists:foreach(fun recover_queue/1, Queues1),
                        ok
                end)
      end).

check_and_maybe_gensym_name(<<"amq.", _/binary>>) ->
    rabbit_misc:die(not_allowed, 'queue.declare');
check_and_maybe_gensym_name(<<>>) ->
    list_to_binary(rabbit_gensym:gensym("amq.q.gen"));
check_and_maybe_gensym_name(NameBin) ->
    NameBin.

declare(RealmName, NameBin, Durable, AutoDelete, Args) ->
    CheckedName = check_and_maybe_gensym_name(NameBin),
    QName = rabbit_misc:r(RealmName, queue, CheckedName),
    Q = start_queue_process(#amqqueue{name = QName,
                                      durable = Durable,
                                      auto_delete = AutoDelete,
                                      arguments = Args,
                                      binding_specs = [],
                                      pid = none}),
    rabbit_misc:execute_simple_mnesia_transaction(
      fun () ->
              ok = recover_queue(Q),
              ok = rabbit_realm:add(RealmName, queue, QName),
              Q
      end).

store_queue(Q = #amqqueue{durable = true}) ->
    ok = mnesia:write(durable_queues, Q, write),
    ok = mnesia:write(Q),
    ok;
store_queue(Q = #amqqueue{durable = false}) ->
    ok = mnesia:write(Q),
    ok.

start_queue_process(Q) ->
    {ok, Pid} = gen_server:start(rabbit_amqqueue_process, Q, []),
    ok = rabbit_monitor:monitor(rabbit_amqqueue_monitor, Pid),
    Q#amqqueue{pid = Pid}.

recover_queue(Q) ->
    ok = store_queue(Q),
    ok = recover_bindings(Q),
    ok.

default_binding_spec(QueueName) ->
    #binding_spec{exchange_name = rabbit_misc:r(QueueName, exchange, <<>>),
                  routing_key = QueueName#resource.name,
                  arguments = []}.

recover_bindings(Q = #amqqueue{name = NameBin, binding_specs = Specs}) ->
    ok = recover_binding(default_binding_spec(NameBin), Q),
    lists:foreach(fun (B) ->
                          ok = recover_binding(B, Q)
                  end, Specs),
    ok.

recover_binding(BindingSpec, Q) ->
    rabbit_exchange:add_binding(BindingSpec, Q).

add_binding(QueueName, ExchangeName, RoutingKey, Arguments) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      fun () ->
              case mnesia:wread({amqqueue, QueueName}) of
                  [] -> {error, not_found};
                  [Q = #amqqueue{binding_specs = Specs0}] ->
                      Spec = #binding_spec{exchange_name = ExchangeName,
                                           routing_key = RoutingKey,
                                           arguments = Arguments},
                      SpecAlreadyPresent = lists:member(Spec, Specs0),
                      if
                          SpecAlreadyPresent -> ok; % ignore duplicates
                          true ->
                              Q1 = Q#amqqueue{binding_specs = [Spec | Specs0]},
                              case recover_binding(Spec, Q1) of
                                  ok -> store_queue(Q1);
                                  Other -> Other
                              end
                      end
              end
      end).

lookup(Name) ->
    rabbit_misc:clean_read({amqqueue, Name}).

lookup_or_die(Name, MethodName) ->
    case lookup(Name) of
        {ok, Q} -> Q;
        {error, not_found} -> rabbit_misc:die(not_found, MethodName)
    end.

list_vhost_queues(VHostPath) ->
    {atomic, Queues} =
        mnesia:transaction(
          fun () ->
                  mnesia:match_object(
                    #amqqueue{name = rabbit_misc:r(VHostPath, queue),
                              _ = '_'})
          end),
    Queues.

stat(#amqqueue{pid = QPid}) -> gen_server:call(QPid, stat).

stat_all() ->
    lists:map(fun stat/1, rabbit_misc:dirty_read_all(amqqueue)).

delete(#amqqueue{ pid = QPid }, IfUnused, IfEmpty) ->
    gen_server:call(QPid, {delete, IfUnused, IfEmpty}).
        
purge(#amqqueue{ pid = QPid }) -> gen_server:call(QPid, purge).

deliver(_IsMandatory, true, Message, QPid) ->
    gen_server:call(QPid, {deliver_immediately, Message});
deliver(true, _IsImmediate, Message, QPid) ->
    gen_server:call(QPid, {deliver, Message}),
    true;
deliver(false, _IsImmediate, Message, QPid) ->
    gen_server:cast(QPid, {deliver, Message}),
    true.

binding_forcibly_removed(BindingSpec, QueueName) ->
    {atomic, Result} =
        mnesia:transaction
          (fun () ->
                   case mnesia:wread({amqqueue, QueueName}) of
                       [] -> ok;
                       [Q = #amqqueue{binding_specs = Specs}] ->
                           store_queue(Q#amqqueue{binding_specs =
                                                  lists:delete(BindingSpec, Specs)})
                   end
           end),
    Result.

claim_queue(#amqqueue{pid = QPid}, ReaderPid) ->
    gen_server:call(QPid, {claim_queue, ReaderPid}).

basic_get(#amqqueue{pid = QPid}) ->
    gen_server:call(QPid, {basic_get}).

basic_consume(#amqqueue{pid = QPid}, NoAck, ReaderPid, WriterPid,
              ConsumerTag, ExclusiveConsume, OkMsg) ->
    gen_server:call(QPid, {basic_consume, NoAck, ReaderPid, WriterPid,
                           ConsumerTag, ExclusiveConsume, OkMsg}).

basic_cancel(#amqqueue{pid = QPid}, WriterPid, ConsumerTag, OkMsg) ->
    ok = gen_server:call(QPid, {basic_cancel, WriterPid, ConsumerTag, OkMsg}).

notify_sent(none) ->
    ok;
notify_sent(QPid) ->
    gen_server:cast(QPid, {notify_sent, self()}).

delete_bindings(Q = #amqqueue{binding_specs = BS}) ->
    delete_bindings(BS, Q).

delete_bindings([], _Q) ->
    ok;
delete_bindings([BS | Rest], Q) ->
    ok = case rabbit_exchange:delete_binding(BS, Q) of
             ok -> ok;
             _ ->
                 rabbit_log:warning("Problem deleting a binding when deleting queue; binding spec ~p, queue ~p~n", [BS, Q]),
                 ok
         end,
    delete_bindings(Rest, Q).

purge_message_buffer(MessageBuffer) ->
    case queue:out(MessageBuffer) of
        {{value, #basic_message{content = #content{}}}, Tail} ->
            %% FIXME: bug 14329: here might be a good place to purge a
            %% persistent message - at the moment this whole function
            %% is a no-op
            purge_message_buffer(Tail);
        {empty, _} ->
            ok
    end.

internal_delete(MessageBuffer, QueueName) ->
    case rabbit_misc:execute_simple_mnesia_transaction(
           fun () ->
                   case mnesia:wread({amqqueue, QueueName}) of
                       [] -> {error, not_found};
                       [Q] ->
                           ok = delete_temp(Q),
                           ok = mnesia:delete({durable_queues, QueueName}),
                           ok = rabbit_realm:delete_from_all(queue, QueueName),
                           ok
                   end
           end) of
        ok ->
            ok = purge_message_buffer(MessageBuffer),
            ok = rabbit_monitor:demonitor(rabbit_amqqueue_monitor, self()),
            ok;
        Other -> Other
    end.
    
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
        true -> ok = rabbit_realm:delete_from_all(queue, QueueName)
    end.

cleanup(Pid) when is_pid(Pid) ->
    ok = rabbit_misc:execute_simple_mnesia_transaction(
      fun () ->
              case mnesia:index_read(amqqueue, Pid, #amqqueue.pid) of
                  [] -> ok;
                  [Q] -> delete_queue(Q)
              end
      end).

on_node_down(Node) ->                      
    rabbit_misc:execute_simple_mnesia_transaction(
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
              pid = Pid}.
