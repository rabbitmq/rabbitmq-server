%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This module is a pseudo queue type.
%% Messages are consumed directly by the declaring MQTT connection process.
%% In a certain sense, the declaring MQTT connection process' mailbox
%% represents a superset of this queue.
%%
%% This queue type allows only stateless clients such that clients (i.e.
%% MQTT publisher connection processes or AMQP 0.9.1 channels) can deliver
%% messages to millions of these queues without requiring a lot of memory.
%%
%% All messages are delivered at most once.

-module(rabbit_mqtt_qos0_queue).
-behaviour(rabbit_queue_type).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

%% Stateless rabbit_queue_type callbacks.
-export([
         declare/2,
         delete/4,
         supports_stateful_delivery/0,
         deliver/3,
         is_enabled/0,
         is_compatible/3,
         is_recoverable/1,
         recover/2,
         purge/1,
         policy_changed/1,
         info/2,
         stat/1,
         format/2,
         capabilities/0,
         notify_decorators/1
        ]).

-export([queue_topology/1,
         feature_flag_name/0,
         policy_apply_to_name/0,
         stop/1,
         list_with_minimum_quorum/0,
         drain/1,
         revive/0,
         queue_vm_stats_sups/0,
         queue_vm_ets/0]).

%% Stateful rabbit_queue_type callbacks are unsupported by this queue type.
-define(STATEFUL_CALLBACKS,
        [
         init/1,
         close/1,
         update/2,
         consume/3,
         cancel/3,
         handle_event/3,
         settle/5,
         credit_v1/5,
         credit/6,
         dequeue/5,
         state_info/1
        ]).
-export(?STATEFUL_CALLBACKS).
-dialyzer({nowarn_function, ?STATEFUL_CALLBACKS}).
-define(UNSUPPORTED(Args), erlang:error(unsupported, Args)).

-define(INFO_KEYS, [type, name, durable, auto_delete, arguments,
                    pid, owner_pid, state, messages]).

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), rabbit_amqqueue:absent_reason()} |
    {protocol_error, internal_error, string(), [string()]}.
declare(Q0, _Node) ->
    QName = amqqueue:get_name(Q0),
    Q1 = case amqqueue:get_pid(Q0) of
             none ->
                 %% declaring process becomes the queue
                 amqqueue:set_pid(Q0, self());
             Pid when is_pid(Pid) ->
                 Q0
         end,
    %% The queue gets persisted such that routing to this
    %% queue (via the topic exchange) works as usual.
    case rabbit_amqqueue:internal_declare(Q1, false) of
        {created, Q} ->
            Opts = amqqueue:get_options(Q),
            ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
            rabbit_event:notify(queue_created,
                                [{name, QName},
                                 {durable, true},
                                 {auto_delete, false},
                                 {exclusive, true},
                                 {type, amqqueue:get_type(Q)},
                                 {arguments, amqqueue:get_arguments(Q)},
                                 {user_who_performed_action, ActingUser}]),
            {new, Q};
        {error, timeout} ->
            {protocol_error, internal_error,
             "Could not declare ~ts because the metadata store operation "
             "timed out",
             [rabbit_misc:rs(QName)]};
        Other ->
            Other
    end.

-spec delete(amqqueue:amqqueue(),
             boolean(),
             boolean(),
             rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(timeout).
delete(Q, _IfUnused, _IfEmpty, ActingUser) ->
    QName = amqqueue:get_name(Q),
    log_delete(QName, amqqueue:get_exclusive_owner(Q)),
    case rabbit_amqqueue:internal_delete(Q, ActingUser) of
        ok ->
            Pid = amqqueue:get_pid(Q),
            gen_server:cast(Pid, {queue_event, ?MODULE, {eol, QName}}),
            {ok, 0};
        {error, timeout} = Err ->
            Err
    end.

supports_stateful_delivery() ->
    false.

-spec deliver([{amqqueue:amqqueue(), stateless}],
              Msg :: mc:state(),
              rabbit_queue_type:delivery_options()) ->
    {[], rabbit_queue_type:actions()}.
deliver(Qs, Msg, Options) ->
    Evt = {queue_event, ?MODULE,
           {?MODULE, _QPid = none, _QMsgId = none, _Redelivered = false, Msg}},
    {Pids, Actions} =
        case maps:get(correlation, Options, undefined) of
            undefined ->
                Pids0 = lists:map(fun({Q, stateless}) -> amqqueue:get_pid(Q) end, Qs),
                {Pids0, []};
            Corr ->
                %% We confirm the message directly here in the queue client.
                %% Alternatively, we could have the target MQTT connection process confirm the message.
                %% However, given that this message might be lost anyway between target MQTT connection
                %% process and MQTT subscriber, and we know that the MQTT subscriber wants to receive
                %% this message at most once, we confirm here directly.
                %% Benefits:
                %% 1. We do not block sending the confirmation back to the publishing client just because a single
                %% (at-most-once) target queue out of potentially many (e.g. million) queues might be unavailable.
                %% 2. Memory usage in this (publishing) process is kept lower because the target queue name can be
                %% directly removed from rabbit_mqtt_confirms and rabbit_confirms.
                %% 3. Reduced network traffic across RabbitMQ nodes.
                %% 4. Lower latency of sending publisher confirmation back to the publishing client.
                Corrs = [Corr],
                lists:mapfoldl(fun({Q, stateless}, Actions) ->
                                       {amqqueue:get_pid(Q),
                                        [{settled, amqqueue:get_name(Q), Corrs}
                                         | Actions]}
                               end, [], Qs)
        end,
    delegate:invoke_no_result(Pids, {gen_server, cast, [Evt]}),
    {[], Actions}.

-spec is_enabled() -> boolean().
is_enabled() -> true.

-spec is_compatible(boolean(), boolean(), boolean()) ->
    boolean().
is_compatible(_Durable = true, _Exclusive = true, _AutoDelete = false) ->
    true;
is_compatible(_, _, _) ->
    false.

-spec is_recoverable(amqqueue:amqqueue()) ->
    boolean().
is_recoverable(Q) ->
    Pid = amqqueue:get_pid(Q),
    OwnerPid = amqqueue:get_exclusive_owner(Q),
    node() =:= node(Pid) andalso
    Pid =:= OwnerPid andalso
    not is_process_alive(Pid).

%% We (mis)use the recover callback to clean up our exclusive queues
%% which otherwise do not get cleaned up after a node crash.
-spec recover(rabbit_types:vhost(), [amqqueue:amqqueue()]) ->
    {Recovered :: [amqqueue:amqqueue()], Failed :: [amqqueue:amqqueue()]}.
recover(_VHost, Queues) ->
    lists:foreach(
      fun(Q) ->
              %% sanity check
              true = is_recoverable(Q),
              QName = amqqueue:get_name(Q),
              log_delete(QName, amqqueue:get_exclusive_owner(Q)),
              rabbit_amqqueue:internal_delete(Q, ?INTERNAL_USER, missing_owner)
      end, Queues),
    %% We mark the queue recovery as failed because these queues are not really
    %% recovered, but deleted.
    {[], Queues}.

log_delete(QName, ConPid) ->
    ?LOG_DEBUG("Deleting ~s of type ~s because its declaring connection ~tp was closed",
               [rabbit_misc:rs(QName), ?MODULE, ConPid], #{domain => ?RMQLOG_DOMAIN_QUEUE}).

-spec purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()}.
purge(_Q) ->
    {ok, 0}.

-spec policy_changed(amqqueue:amqqueue()) ->
    ok.
policy_changed(_Q) ->
    ok.

-spec notify_decorators(amqqueue:amqqueue()) ->
    ok.
notify_decorators(_) ->
    ok.

-spec stat(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer(), non_neg_integer()}.
stat(_Q) ->
    {ok, 0, 0}.

-spec format(amqqueue:amqqueue(), map()) ->
    [{atom(), term()}].
format(Q, _Ctx) ->
    [{type, ?MODULE},
     {state, amqqueue:get_state(Q)}].

capabilities() ->
    #{can_redeliver => false,
      consumer_arguments => [],
      is_replicable => false,
      queue_arguments => [],
      rebalance_module => undefined,
      server_named => true,
      unsupported_policies => []}.

-spec info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(Q, all_keys)
  when ?is_amqqueue(Q) ->
    info(Q, ?INFO_KEYS);
info(Q, Items)
  when ?is_amqqueue(Q) ->
    [{Item, i(Item, Q)} || Item <- Items].

i(type, _) ->
    'MQTT QoS 0';
i(name, Q) ->
    amqqueue:get_name(Q);
i(durable, Q) ->
    amqqueue:is_durable(Q);
i(auto_delete, Q) ->
    amqqueue:is_auto_delete(Q);
i(arguments, Q) ->
    amqqueue:get_arguments(Q);
i(pid, Q) ->
    amqqueue:get_pid(Q);
i(owner_pid, Q) ->
    amqqueue:get_exclusive_owner(Q);
i(state, Q) ->
    Pid = amqqueue:get_pid(Q),
    case erpc:call(node(Pid), erlang, is_process_alive, [Pid]) of
        true ->
            running;
        false ->
            down
    end;
i(messages, Q) ->
    Pid = amqqueue:get_pid(Q),
    case erpc:call(node(Pid), erlang, process_info, [Pid, message_queue_len]) of
        {message_queue_len, N} ->
            N;
        _ ->
            0
    end;
i(_, _) ->
    ''.

init(A1) ->
    ?UNSUPPORTED([A1]).

close(A1) ->
    ?UNSUPPORTED([A1]).

update(A1,A2) ->
    ?UNSUPPORTED([A1,A2]).

consume(A1,A2,A3) ->
    ?UNSUPPORTED([A1,A2,A3]).

cancel(A1,A2,A3) ->
    ?UNSUPPORTED([A1,A2,A3]).

handle_event(A1,A2,A3) ->
    ?UNSUPPORTED([A1,A2,A3]).

settle(A1,A2,A3,A4,A5) ->
    ?UNSUPPORTED([A1,A2,A3,A4,A5]).

credit_v1(A1,A2,A3,A4,A5) ->
    ?UNSUPPORTED([A1,A2,A3,A4,A5]).

credit(A1,A2,A3,A4,A5,A6) ->
    ?UNSUPPORTED([A1,A2,A3,A4,A5,A6]).

dequeue(A1,A2,A3,A4,A5) ->
    ?UNSUPPORTED([A1,A2,A3,A4,A5]).

state_info(A1) ->
    ?UNSUPPORTED([A1]).

-spec queue_topology(amqqueue:amqqueue()) ->
    {Leader :: undefined | node(), Replicas :: undefined | [node(),...]}.
queue_topology(Q) ->
    Pid = amqqueue:get_pid(Q),
    Node = node(Pid),
    {Node, [Node]}.

feature_flag_name() ->
    undefined.

policy_apply_to_name() ->
    <<"qos0_queues">>.

stop(_VHost) ->
    ok.

list_with_minimum_quorum() ->
    [].

drain(_TransferCandidates) ->
    ok.

revive() ->
    ok.

queue_vm_stats_sups() ->
    {[], []}.

queue_vm_ets() ->
    {[], []}.
