%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
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

%% Stateless rabbit_queue_type callbacks.
-export([
         is_stateful/0,
         declare/2,
         delete/4,
         deliver/2,
         is_enabled/0,
         is_compatible/3,
         is_recoverable/1,
         recover/2,
         purge/1,
         policy_changed/1,
         info/2,
         stat/1,
         capabilities/0,
         notify_decorators/1
        ]).

%% Stateful rabbit_queue_type callbacks are unsupported by this queue type.
-define(STATEFUL_CALLBACKS,
        [
         init/1,
         close/1,
         update/2,
         consume/3,
         cancel/5,
         handle_event/3,
         settle/5,
         credit/5,
         dequeue/5,
         state_info/1
        ]).
-export(?STATEFUL_CALLBACKS).
-dialyzer({nowarn_function, ?STATEFUL_CALLBACKS}).
-define(UNSUPPORTED(Args), erlang:error(unsupported, Args)).

-define(INFO_KEYS, [type, name, durable, auto_delete, arguments,
                    pid, owner_pid, state, messages]).

-spec is_stateful() ->
    boolean().
is_stateful() ->
    false.

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), rabbit_amqqueue:absent_reason()}.
declare(Q0, _Node) ->
    %% The queue gets persisted such that routing to this
    %% queue (via the topic exchange) works as usual.
    case rabbit_amqqueue:internal_declare(Q0, false) of
        {created, Q} ->
            Opts = amqqueue:get_options(Q),
            ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
            rabbit_event:notify(queue_created,
                                [{name, amqqueue:get_name(Q0)},
                                 {durable, true},
                                 {auto_delete, false},
                                 {exclusive, true},
                                 {type, amqqueue:get_type(Q0)},
                                 {arguments, amqqueue:get_arguments(Q0)},
                                 {user_who_performed_action, ActingUser}]),
            {new, Q};
        Other ->
            Other
    end.

-spec delete(amqqueue:amqqueue(),
             boolean(),
             boolean(),
             rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()).
delete(Q, _IfUnused, _IfEmpty, ActingUser) ->
    QName = amqqueue:get_name(Q),
    log_delete(QName, amqqueue:get_exclusive_owner(Q)),
    ok = rabbit_amqqueue:internal_delete(Q, ActingUser),
    {ok, 0}.

-spec deliver([{amqqueue:amqqueue(), stateless}], Delivery :: term()) ->
    {[], rabbit_queue_type:actions()}.
deliver(Qs, #delivery{message = BasicMessage,
                      confirm = Confirm,
                      msg_seq_no = SeqNo}) ->
    Msg = {queue_event, ?MODULE,
           {?MODULE, _QPid = none, _QMsgId = none, _Redelivered = false, BasicMessage}},
    {Pids, Actions} =
    case Confirm of
        false ->
            Pids0 = lists:map(fun({Q, stateless}) -> amqqueue:get_pid(Q) end, Qs),
            {Pids0, []};
        true ->
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
            SeqNos = [SeqNo],
            lists:mapfoldl(fun({Q, stateless}, Actions) ->
                                   {amqqueue:get_pid(Q),
                                    [{settled, amqqueue:get_name(Q), SeqNos} | Actions]}
                           end, [], Qs)
    end,
    delegate:invoke_no_result(Pids, {gen_server, cast, [Msg]}),
    {[], Actions}.

-spec is_enabled() -> boolean().
is_enabled() -> rabbit_feature_flags:is_enabled(?MODULE).

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
    rabbit_log_queue:debug("Deleting ~s of type ~s because its declaring connection ~tp was closed",
                           [rabbit_misc:rs(QName), ?MODULE, ConPid]).

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

-spec capabilities() ->
    #{atom() := term()}.
capabilities() ->
    #{}.

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

cancel(A1,A2,A3,A4,A5) ->
    ?UNSUPPORTED([A1,A2,A3,A4,A5]).

handle_event(A1,A2,A3) ->
    ?UNSUPPORTED([A1,A2,A3]).

settle(A1,A2,A3,A4,A5) ->
    ?UNSUPPORTED([A1,A2,A3,A4,A5]).

credit(A1,A2,A3,A4,A5) ->
    ?UNSUPPORTED([A1,A2,A3,A4,A5]).

dequeue(A1,A2,A3,A4,A5) ->
    ?UNSUPPORTED([A1,A2,A3,A4,A5]).

state_info(A1) ->
    ?UNSUPPORTED([A1]).
