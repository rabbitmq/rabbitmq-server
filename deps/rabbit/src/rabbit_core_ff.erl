%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_core_ff).

-rabbit_feature_flag(
   {classic_mirrored_queue_version,
    #{desc          => "Support setting version for classic mirrored queues",
      stability     => required
     }}).

-rabbit_feature_flag(
   {quorum_queue,
    #{desc          => "Support queues of type `quorum`",
      doc_url       => "https://www.rabbitmq.com/quorum-queues.html",
      stability     => required
     }}).

-rabbit_feature_flag(
   {stream_queue,
    #{desc          => "Support queues of type `stream`",
      doc_url       => "https://www.rabbitmq.com/stream.html",
      stability     => required,
      depends_on    => [quorum_queue]
     }}).

-rabbit_feature_flag(
   {implicit_default_bindings,
    #{desc          => "Default bindings are now implicit, instead of "
                       "being stored in the database",
      stability     => required
     }}).

-rabbit_feature_flag(
   {virtual_host_metadata,
    #{desc          => "Virtual host metadata (description, tags, etc)",
      stability     => required
     }}).

-rabbit_feature_flag(
   {maintenance_mode_status,
    #{desc          => "Maintenance mode status",
      stability     => required
     }}).

-rabbit_feature_flag(
    {user_limits,
     #{desc          => "Configure connection and channel limits for a user",
       stability     => required
     }}).

-rabbit_feature_flag(
   {stream_single_active_consumer,
    #{desc          => "Single active consumer for streams",
      doc_url       => "https://www.rabbitmq.com/stream.html",
      stability     => required,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
    {feature_flags_v2,
     #{desc          => "Feature flags subsystem V2",
       stability     => required
     }}).

-rabbit_feature_flag(
   {direct_exchange_routing_v2,
    #{desc       => "v2 direct exchange routing implementation",
      stability  => required,
      depends_on => [feature_flags_v2, implicit_default_bindings]
     }}).

-rabbit_feature_flag(
   {listener_records_in_ets,
    #{desc       => "Store listener records in ETS instead of Mnesia",
      stability  => required,
      depends_on => [feature_flags_v2]
     }}).

-rabbit_feature_flag(
   {tracking_records_in_ets,
    #{desc          => "Store tracking records in ETS instead of Mnesia",
      stability     => required,
      depends_on    => [feature_flags_v2]
     }}).

-rabbit_feature_flag(
   {classic_queue_type_delivery_support,
    #{desc          => "Bug fix for classic queue deliveries using mixed versions",
      doc_url       => "https://github.com/rabbitmq/rabbitmq-server/issues/5931",
      %%TODO remove compatibility code
      stability     => required,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
   {restart_streams,
    #{desc          => "Support for restarting streams with optional preferred next leader argument. "
      "Used to implement stream leader rebalancing",
      stability     => stable,
      depends_on    => [stream_queue]
     }}).


-rabbit_feature_flag(
   {stream_sac_coordinator_unblock_group,
    #{desc          => "Bug fix to unblock a group of consumers in a super stream partition",
      doc_url       => "https://github.com/rabbitmq/rabbitmq-server/issues/7743",
      stability     => stable,
      depends_on    => [stream_single_active_consumer]
     }}).
