%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store).

-export([behaviour_info/1, table_name_for/1]).

behaviour_info(callbacks) ->
    [{new,       2},
     {recover,   2},
     {insert,    3},
     {lookup,    2},
     {delete,    2},
     {terminate, 1}];
behaviour_info(_Other) ->
    undefined.

table_name_for(VHost) ->
  rabbit_mqtt_util:vhost_name_to_table_name(VHost).

%% TODO
%% Support retained messages in RabbitMQ cluster:
%% * SUBSCRIBE on a different node than PUBLISH with retain
%% * replicate retained message for data safety
%%
%% Possible solutions:
%% * rabbitmq_mqtt_retained_msg_store_mnesia
%% * rabbitmq_mqtt_retained_msg_store_khepri
%% * rabbitmq_mqtt_retained_msg_store_ra (implementing our own ra machine) or
%% * use existing mqtt_machine
