%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

%% rabbit_queue_type callbacks
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

-spec is_stateful() ->
    boolean().
is_stateful() ->
    false.

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), rabbit_queue_type:absent_reason()}.
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
    ok = rabbit_amqqueue:internal_delete(QName, ActingUser),
    {ok, 0}.

%%TODO implement some form of flow control to not overwhelm the destination
%% MQTT connection process?
%% E.g. drop this message if destination queue is already long?
% erlang:process_info(Pid, message_queue_len)
%% However that seems to be expensive due to locking (see lcnt).
%% Alternatively, use credits? use rabbit_amqqueue_common:notify_sent/2 on the consuming side?
-spec deliver([{amqqueue:amqqueue(), stateless}], Delivery :: term()) ->
    {[], rabbit_queue_type:actions()}.
deliver([{Q, stateless}], Delivery = #delivery{message = BasicMessage}) ->
    Pid = amqqueue:get_pid(Q),
    Msg = {queue_event, ?MODULE,
           {?MODULE, Pid, _QMsgId = none, _Redelivered = false, BasicMessage}},
    gen_server:cast(Pid, Msg),
    Actions = confirm(Delivery, Q),
    {[], Actions}.

confirm(#delivery{confirm = false}, _) ->
    [];
confirm(#delivery{confirm = true,
                  msg_seq_no = SeqNo}, Q) ->
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
    QName = amqqueue:get_name(Q),
    [{settled, QName, [SeqNo]}].

-spec is_enabled() ->
    boolean().
is_enabled() ->
    true.

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
              rabbit_amqqueue:internal_delete(QName, ?INTERNAL_USER)
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

%% general queue info
-spec info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(_Q, _Items) ->
    [].

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
