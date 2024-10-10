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

-define(KHEPRI_ROOT_PATH, ?KHEPRI_ROOT_PATH([])).
-define(KHEPRI_ROOT_PATH(Rest), [rabbitmq | Rest]).

-define(KHEPRI_MAINTENANCE_PATH(Node),
        ?KHEPRI_ROOT_PATH([node_maintenance, Node])).

-define(KHEPRI_GLOBAL_RUNTIME_PARAM_PATH(Key),
        ?KHEPRI_ROOT_PATH([runtime_params, Key])).

-define(KHEPRI_USER_PATH(Username),
        ?KHEPRI_ROOT_PATH([users, Username])).

-define(KHEPRI_MIRRORED_SUPERVISOR_PATH(Group, Id),
        ?KHEPRI_ROOT_PATH([mirrored_supervisors, Group, Id])).

-define(KHEPRI_VHOST_PATH(Name),
        ?KHEPRI_VHOST_PATH(Name, [])).
-define(KHEPRI_VHOST_PATH(Name, Rest),
        ?KHEPRI_ROOT_PATH([vhosts, Name | Rest])).

-define(KHEPRI_VHOST_RUNTIME_PARAM_PATH(VHost, Component, Name),
        ?KHEPRI_VHOST_PATH(VHost, [runtime_params, Component, Name])).

-define(KHEPRI_USER_PERMISSION_PATH(VHost, Username),
        ?KHEPRI_VHOST_PATH(VHost, [user_permissions, Username])).

-define(KHEPRI_EXCHANGE_PATH(VHost, Name),
        ?KHEPRI_EXCHANGE_PATH(VHost, Name, [])).
-define(KHEPRI_EXCHANGE_PATH(VHost, Name, Rest),
        ?KHEPRI_VHOST_PATH(VHost, [exchanges, Name | Rest])).

-define(KHEPRI_EXCHANGE_SERIAL_PATH(VHost, Name),
        ?KHEPRI_EXCHANGE_PATH(VHost, Name, [serial])).

-define(KHEPRI_TOPIC_PERMISSION_PATH(VHost, Exchange, Username),
        ?KHEPRI_EXCHANGE_PATH(VHost, Exchange, [user_permissions, Username])).

-define(KHEPRI_ROUTE_PATH(VHost, SrcName, Kind, DstName, RoutingKey),
        ?KHEPRI_EXCHANGE_PATH(
          VHost, SrcName, [bindings, Kind, DstName, RoutingKey])).

-define(KHEPRI_QUEUE_PATH(VHost, Name),
        ?KHEPRI_VHOST_PATH(VHost, [queues, Name])).
