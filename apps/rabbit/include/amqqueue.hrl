%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-include("amqqueue_v1.hrl").
-include("amqqueue_v2.hrl").

-define(is_amqqueue(Q),
        (?is_amqqueue_v2(Q) orelse
         ?is_amqqueue_v1(Q))).

-define(amqqueue_is_auto_delete(Q),
        ((?is_amqqueue_v2(Q) andalso
          ?amqqueue_v2_field_auto_delete(Q) =:= true) orelse
         (?is_amqqueue_v1(Q) andalso
          ?amqqueue_v1_field_auto_delete(Q) =:= true))).

-define(amqqueue_is_durable(Q),
        ((?is_amqqueue_v2(Q) andalso
          ?amqqueue_v2_field_durable(Q) =:= true) orelse
         (?is_amqqueue_v1(Q) andalso
          ?amqqueue_v1_field_durable(Q) =:= true))).

-define(amqqueue_exclusive_owner_is(Q, Owner),
        ((?is_amqqueue_v2(Q) andalso
          ?amqqueue_v2_field_exclusive_owner(Q) =:= Owner) orelse
         (?is_amqqueue_v1(Q) andalso
          ?amqqueue_v1_field_exclusive_owner(Q) =:= Owner))).

-define(amqqueue_exclusive_owner_is_pid(Q),
        ((?is_amqqueue_v2(Q) andalso
          is_pid(?amqqueue_v2_field_exclusive_owner(Q))) orelse
         (?is_amqqueue_v1(Q) andalso
          is_pid(?amqqueue_v1_field_exclusive_owner(Q))))).

-define(amqqueue_state_is(Q, State),
        ((?is_amqqueue_v2(Q) andalso
          ?amqqueue_v2_field_state(Q) =:= State) orelse
         (?is_amqqueue_v1(Q) andalso
          ?amqqueue_v1_field_state(Q) =:= State))).

-define(amqqueue_v1_type, rabbit_classic_queue).

-define(amqqueue_is_classic(Q),
        ((?is_amqqueue_v2(Q) andalso
          ?amqqueue_v2_field_type(Q) =:= rabbit_classic_queue) orelse
         ?is_amqqueue_v1(Q))).

-define(amqqueue_is_quorum(Q),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_field_type(Q) =:= rabbit_quorum_queue) orelse
        false).

-define(amqqueue_is_stream(Q),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_field_type(Q) =:= rabbit_stream_queue) orelse
        false).

-define(amqqueue_has_valid_pid(Q),
        ((?is_amqqueue_v2(Q) andalso
          is_pid(?amqqueue_v2_field_pid(Q))) orelse
         (?is_amqqueue_v1(Q) andalso
          is_pid(?amqqueue_v1_field_pid(Q))))).

-define(amqqueue_pid_runs_on_local_node(Q),
        ((?is_amqqueue_v2(Q) andalso
          node(?amqqueue_v2_field_pid(Q)) =:= node()) orelse
         (?is_amqqueue_v1(Q) andalso
          node(?amqqueue_v1_field_pid(Q)) =:= node()))).

-define(amqqueue_pid_equals(Q, Pid),
        ((?is_amqqueue_v2(Q) andalso
          ?amqqueue_v2_field_pid(Q) =:= Pid) orelse
         (?is_amqqueue_v1(Q) andalso
          ?amqqueue_v1_field_pid(Q) =:= Pid))).

-define(amqqueue_pids_are_equal(Q0, Q1),
        ((?is_amqqueue_v2(Q0) andalso ?is_amqqueue_v2(Q1) andalso
          ?amqqueue_v2_field_pid(Q0) =:= ?amqqueue_v2_field_pid(Q1)) orelse
         (?is_amqqueue_v1(Q0) andalso ?is_amqqueue_v1(Q1) andalso
          ?amqqueue_v1_field_pid(Q0) =:= ?amqqueue_v1_field_pid(Q1)))).

-define(amqqueue_field_name(Q),
        case ?is_amqqueue_v2(Q) of
            true  -> ?amqqueue_v2_field_name(Q);
            false -> case ?is_amqqueue_v1(Q) of
                         true -> ?amqqueue_v1_field_name(Q)
                     end
        end).

-define(amqqueue_field_pid(Q),
        case ?is_amqqueue_v2(Q) of
            true  -> ?amqqueue_v2_field_pid(Q);
            false -> case ?is_amqqueue_v1(Q) of
                         true -> ?amqqueue_v1_field_pid(Q)
                     end
        end).

-define(amqqueue_v1_vhost(Q), element(2, ?amqqueue_v1_field_name(Q))).
-define(amqqueue_v2_vhost(Q), element(2, ?amqqueue_v2_field_name(Q))).

-define(amqqueue_vhost_equals(Q, VHost),
        ((?is_amqqueue_v2(Q) andalso
          ?amqqueue_v2_vhost(Q) =:= VHost) orelse
         (?is_amqqueue_v1(Q) andalso
          ?amqqueue_v1_vhost(Q) =:= VHost))).

-ifdef(DEBUG_QUORUM_QUEUE_FF).
-define(enable_quorum_queue_if_debug,
        begin
            rabbit_log:info(
              "---- ENABLING quorum_queue as part of "
              "?try_mnesia_tx_or_upgrade_amqqueue_and_retry() ----"),
            ok = rabbit_feature_flags:enable(quorum_queue)
        end).
-else.
-define(enable_quorum_queue_if_debug, noop).
-endif.

-define(try_mnesia_tx_or_upgrade_amqqueue_and_retry(Expr1, Expr2),
        try
            ?enable_quorum_queue_if_debug,
            Expr1
        catch
            throw:{error, {bad_type, T}} when ?is_amqqueue(T) ->
                Expr2;
            throw:{aborted, {bad_type, T}} when ?is_amqqueue(T) ->
                Expr2
        end).
