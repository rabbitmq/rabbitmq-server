%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-type vhost() :: binary().
-type partition_index() :: integer().
-type stream() :: binary().
-type consumer_name() :: binary().
-type connection_pid() :: pid().
-type subscription_id() :: byte().
-type group_id() :: {vhost(), stream(), consumer_name()}.
-type owner() :: binary().

-record(consumer,
        {pid :: pid(),
         subscription_id :: subscription_id(),
         owner :: owner(), %% just a label
         active :: boolean()}).
-record(group,
        {consumers :: [#consumer{}], partition_index :: integer()}).
-record(rabbit_stream_sac_coordinator,
        {groups :: #{group_id() => #group{}},
         pids_groups ::
             #{connection_pid() =>
                   #{group_id() => true}}, %% inner map acts as a set
         %% future extensibility
         reserved_1,
         reserved_2}).
%% commands
-record(command_register_consumer,
        {vhost :: vhost(),
         stream :: stream(),
         partition_index :: partition_index(),
         consumer_name :: consumer_name(),
         connection_pid :: connection_pid(),
         owner :: owner(),
         subscription_id :: subscription_id()}).
-record(command_unregister_consumer,
        {vhost :: vhost(),
         stream :: stream(),
         consumer_name :: consumer_name(),
         connection_pid :: connection_pid(),
         subscription_id :: subscription_id()}).
-record(command_activate_consumer,
        {vhost :: vhost(), stream :: stream(),
         consumer_name :: consumer_name()}).
