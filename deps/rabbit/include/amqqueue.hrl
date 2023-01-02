%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-include("amqqueue_v2.hrl").

-define(is_amqqueue(Q),
        (?is_amqqueue_v2(Q))).

-define(amqqueue_is_auto_delete(Q),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_field_auto_delete(Q) =:= true)).

-define(amqqueue_is_durable(Q),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_field_durable(Q) =:= true)).

-define(amqqueue_exclusive_owner_is(Q, Owner),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_field_exclusive_owner(Q) =:= Owner)).

-define(amqqueue_exclusive_owner_is_pid(Q),
        (?is_amqqueue_v2(Q) andalso
         is_pid(?amqqueue_v2_field_exclusive_owner(Q)))).

-define(amqqueue_state_is(Q, State),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_field_state(Q) =:= State)).

-define(amqqueue_v1_type, rabbit_classic_queue).

-define(amqqueue_is_classic(Q),
        ?amqqueue_type_is(Q, rabbit_classic_queue)).

-define(amqqueue_is_quorum(Q),
        ?amqqueue_type_is(Q, rabbit_quorum_queue)).

-define(amqqueue_is_stream(Q),
        ?amqqueue_type_is(Q, rabbit_stream_queue)).

-define(amqqueue_type_is(Q, Type),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_field_type(Q) =:= Type)).

-define(amqqueue_has_valid_pid(Q),
        (?is_amqqueue_v2(Q) andalso
         is_pid(?amqqueue_v2_field_pid(Q)))).

-define(amqqueue_pid_runs_on_local_node(Q),
        (?is_amqqueue_v2(Q) andalso
         node(?amqqueue_v2_field_pid(Q)) =:= node())).

-define(amqqueue_pid_equals(Q, Pid),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_field_pid(Q) =:= Pid)).

-define(amqqueue_pids_are_equal(Q0, Q1),
        (?is_amqqueue_v2(Q0) andalso ?is_amqqueue_v2(Q1) andalso
         ?amqqueue_v2_field_pid(Q0) =:= ?amqqueue_v2_field_pid(Q1))).

-define(amqqueue_field_name(Q),
        ?amqqueue_v2_field_name(Q)).

-define(amqqueue_field_pid(Q),
        ?amqqueue_v2_field_pid(Q)).

-define(amqqueue_v2_vhost(Q), element(2, ?amqqueue_v2_field_name(Q))).

-define(amqqueue_vhost_equals(Q, VHost),
        (?is_amqqueue_v2(Q) andalso
         ?amqqueue_v2_vhost(Q) =:= VHost)).
