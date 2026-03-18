%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_queue_type_util).

-export([args_policy_lookup/3,
         qname_to_internal_name/1,
         check_auto_delete/1,
         check_exclusive/1,
         check_non_durable/1,
         erpc_call/5,
         local_or_remote_handler/4,
         notify_consumer_created/9,
         notify_consumer_deleted/4,
         get_connected_nodes/1,
         coarse_message_counts/1,
         wait_for_projection/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

args_policy_lookup(Name, Resolve, Q) when ?is_amqqueue(Q) ->
    Args = amqqueue:get_arguments(Q),
    AName = <<"x-", Name/binary>>,
    case {rabbit_policy:get(Name, Q), rabbit_misc:table_lookup(Args, AName)} of
        {undefined, undefined}       -> undefined;
        {undefined, {_Type, Val}}    -> Val;
        {Val,       undefined}       -> Val;
        {PolVal,    {_Type, ArgVal}} -> Resolve(PolVal, ArgVal)
    end.

qname_to_internal_name(QName) ->
    case name_concat(QName) of
        Name when byte_size(Name) =< 255 ->
            {ok, erlang:binary_to_atom(Name)};
        Name ->
            {error, {too_long, Name}}
    end.

name_concat(#resource{virtual_host = <<"/">>, name = Name}) ->
    <<"%2F_", Name/binary>>;
name_concat(#resource{virtual_host = VHost, name = Name}) ->
    <<VHost/binary, "_", Name/binary>>.

check_auto_delete(Q) when ?amqqueue_is_auto_delete(Q) ->
    Name = amqqueue:get_name(Q),
    {protocol_error, precondition_failed, "invalid property 'auto-delete' for ~ts",
     [rabbit_misc:rs(Name)]};
check_auto_delete(_) ->
    ok.

check_exclusive(Q) when ?amqqueue_exclusive_owner_is(Q, none) ->
    ok;
check_exclusive(Q) when ?is_amqqueue(Q) ->
    Name = amqqueue:get_name(Q),
    {protocol_error, precondition_failed, "invalid property 'exclusive-owner' for ~ts",
     [rabbit_misc:rs(Name)]}.

check_non_durable(Q) when ?amqqueue_is_durable(Q) ->
    ok;
check_non_durable(Q) when not ?amqqueue_is_durable(Q) ->
    Name = amqqueue:get_name(Q),
    {protocol_error, precondition_failed, "invalid property 'non-durable' for ~ts",
     [rabbit_misc:rs(Name)]}.

-spec erpc_call(node(), module(), atom(), list(), non_neg_integer() | infinity) ->
    term() | {error, term()}.
erpc_call(Node, M, F, A, _Timeout)
  when Node =:= node()  ->
    %% Only timeout 'infinity' optimises the local call in OTP 23-25 avoiding a new process being spawned:
    %% https://github.com/erlang/otp/blob/47f121af8ee55a0dbe2a8c9ab85031ba052bad6b/lib/kernel/src/erpc.erl#L121
    try erpc:call(Node, M, F, A, infinity) of
        Result ->
            Result
    catch
        error:Err ->
            {error, Err}
    end;
erpc_call(Node, M, F, A, Timeout) ->
    case lists:member(Node, nodes()) of
        true ->
            try erpc:call(Node, M, F, A, Timeout) of
                Result ->
                    Result
            catch
                error:Err ->
                    {error, Err}
            end;
        false ->
            {error, noconnection}
    end.

-spec local_or_remote_handler(pid(), module(), atom(), [term()]) -> term().
local_or_remote_handler(Pid, Module, Function, Args) ->
    Node = node(Pid),
    case Node == node() of
        true ->
            erlang:apply(Module, Function, Args);
        false ->
            %% This could potentially block for a while if the node is
            %% in disconnected state or tcp buffers are full.
            erpc:cast(Node, Module, Function, Args)
    end.

-spec notify_consumer_created(pid(),
                              rabbit_types:ctag(),
                              boolean(),
                              boolean(),
                              rabbit_amqqueue:name(),
                              non_neg_integer(),
                              rabbit_framing:amqp_table(),
                              reference() | none,
                              rabbit_types:username()) -> ok.
notify_consumer_created(ChPid, CTag, Exclusive, AckRequired, QName,
                        PrefetchCount, Args, Ref, ActingUser) ->
    Props = [{consumer_tag, CTag},
             {exclusive, Exclusive},
             {ack_required, AckRequired},
             {channel, ChPid},
             {queue, QName},
             {prefetch_count, PrefetchCount},
             {arguments, Args},
             {user_who_performed_action, ActingUser}],
    rabbit_event:notify(consumer_created, Props, Ref).

-spec notify_consumer_deleted(pid(),
                              rabbit_types:ctag(),
                              rabbit_amqqueue:name(),
                              rabbit_types:username()) -> ok.
notify_consumer_deleted(ChPid, ConsumerTag, QName, ActingUser) ->
    Props = [{consumer_tag, ConsumerTag},
             {channel, ChPid},
             {queue, QName},
             {user_who_performed_action, ActingUser}],
    rabbit_event:notify(consumer_deleted, Props).

-spec get_connected_nodes(amqqueue:amqqueue()) -> [node()].
get_connected_nodes(Q) when ?is_amqqueue(Q) ->
    ErlangNodes = [node() | nodes()],
    [N || N <- rabbit_amqqueue:get_quorum_nodes(Q),
          lists:member(N, ErlangNodes)].

-spec coarse_message_counts(amqqueue:amqqueue()) -> rabbit_types:infos().
coarse_message_counts(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, MR, MU, M, _}] ->
            [{messages_ready, MR},
             {messages_unacknowledged, MU},
             {messages, M}];
        [] ->
            [{messages_ready, 0},
             {messages_unacknowledged, 0},
             {messages, 0}]
    end.

-spec wait_for_projection(node(), rabbit_amqqueue:name()) -> ok | no_return().
wait_for_projection(Node, QName) ->
    case rabbit_feature_flags:is_enabled(khepri_db) andalso Node =/= node() of
        true ->
            wait_for_remote_projection(Node, QName, 256);
        false ->
            ok
    end.

wait_for_remote_projection(Node, QName, 0) ->
    exit({timeout, ?FUNCTION_NAME, Node, QName});
wait_for_remote_projection(Node, QName, N) ->
    case erpc_call(Node, rabbit_amqqueue, lookup, [QName], 100) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(100),
            wait_for_remote_projection(Node, QName, N - 1)
    end.
