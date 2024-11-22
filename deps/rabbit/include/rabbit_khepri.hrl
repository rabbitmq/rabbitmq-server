%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2024 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This header has macros that define the `khepri_path:native_pattern()'
%% path patterns used for each piece of metadata in the store. We use macros
%% for these so that we can pattern match on these path patterns as well as
%% create them as new terms.
%%
%% If you are creating a path pattern to use in a call to the Khepri API (for
%% example `rabbit_khepri:get/1') you should prefer using the
%% `khepri_<entity>_path' function from the `rabbit_db_<entity>' modules
%% instead, for example `rabbit_db_queue:khepri_queue_path/2', since those
%% functions have guards to assert that the variables passed are valid pattern
%% components.

-define(RABBITMQ_KHEPRI_ROOT_PATH, ?RABBITMQ_KHEPRI_ROOT_PATH([])).
-define(RABBITMQ_KHEPRI_ROOT_PATH(Rest), [rabbitmq | Rest]).

-define(RABBITMQ_KHEPRI_MAINTENANCE_PATH(Node),
        ?RABBITMQ_KHEPRI_ROOT_PATH([node_maintenance, Node])).

-define(RABBITMQ_KHEPRI_GLOBAL_RUNTIME_PARAM_PATH(Key),
        ?RABBITMQ_KHEPRI_ROOT_PATH([runtime_params, Key])).

-define(RABBITMQ_KHEPRI_USER_PATH(Username),
        ?RABBITMQ_KHEPRI_ROOT_PATH([users, Username])).

-define(RABBITMQ_KHEPRI_MIRRORED_SUPERVISOR_PATH(Group, Id),
        ?RABBITMQ_KHEPRI_ROOT_PATH([mirrored_supervisors, Group, Id])).

-define(RABBITMQ_KHEPRI_VHOST_PATH(Name),
        ?RABBITMQ_KHEPRI_VHOST_PATH(Name, [])).
-define(RABBITMQ_KHEPRI_VHOST_PATH(Name, Rest),
        ?RABBITMQ_KHEPRI_ROOT_PATH([vhosts, Name | Rest])).

-define(RABBITMQ_KHEPRI_VHOST_RUNTIME_PARAM_PATH(VHost, Component, Name),
        ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [runtime_params, Component, Name])).

-define(RABBITMQ_KHEPRI_USER_PERMISSION_PATH(VHost, Username),
        ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [user_permissions, Username])).

-define(RABBITMQ_KHEPRI_EXCHANGE_PATH(VHost, Name),
        ?RABBITMQ_KHEPRI_EXCHANGE_PATH(VHost, Name, [])).
-define(RABBITMQ_KHEPRI_EXCHANGE_PATH(VHost, Name, Rest),
        ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [exchanges, Name | Rest])).

-define(RABBITMQ_KHEPRI_EXCHANGE_SERIAL_PATH(VHost, Name),
        ?RABBITMQ_KHEPRI_EXCHANGE_PATH(VHost, Name, [serial])).

-define(RABBITMQ_KHEPRI_TOPIC_PERMISSION_PATH(VHost, Exchange, Username),
        ?RABBITMQ_KHEPRI_EXCHANGE_PATH(
          VHost, Exchange, [user_permissions, Username])).

-define(RABBITMQ_KHEPRI_ROUTE_PATH(VHost, SrcName, Kind, DstName, RoutingKey),
        ?RABBITMQ_KHEPRI_EXCHANGE_PATH(
          VHost, SrcName, [bindings, Kind, DstName, RoutingKey])).

-define(RABBITMQ_KHEPRI_QUEUE_PATH(VHost, Name),
        ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [queues, Name])).
