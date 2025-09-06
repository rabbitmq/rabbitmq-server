%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_core_ff).

-rabbit_feature_flag(
   {classic_mirrored_queue_version,
    #{desc          => "Support setting version for classic mirrored queues",
      stability     => required,
      require_level => hard
     }}).

-rabbit_feature_flag(
   {quorum_queue,
    #{desc          => "Support queues of type `quorum`",
      doc_url       => "https://www.rabbitmq.com/docs/quorum-queues",
      stability     => required,
      require_level => hard
     }}).

-rabbit_feature_flag(
   {stream_queue,
    #{desc          => "Support queues of type `stream`",
      doc_url       => "https://www.rabbitmq.com/docs/stream",
      stability     => required,
      require_level => hard,
      depends_on    => [quorum_queue]
     }}).

-rabbit_feature_flag(
   {implicit_default_bindings,
    #{desc          => "Default bindings are now implicit, instead of "
                       "being stored in the database",
      stability     => required,
      require_level => hard
     }}).

-rabbit_feature_flag(
   {virtual_host_metadata,
    #{desc          => "Virtual host metadata (description, tags, etc)",
      stability     => required,
      require_level => hard
     }}).

-rabbit_feature_flag(
   {maintenance_mode_status,
    #{desc          => "Maintenance mode status",
      stability     => required,
      require_level => hard
     }}).

-rabbit_feature_flag(
   {user_limits,
    #{desc          => "Configure connection and channel limits for a user",
      stability     => required,
      require_level => hard
     }}).

-rabbit_feature_flag(
   {stream_single_active_consumer,
    #{desc          => "Single active consumer for streams",
      doc_url       => "https://www.rabbitmq.com/docs/stream",
      stability     => required,
      require_level => hard,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
   {feature_flags_v2,
    #{desc          => "Feature flags subsystem V2",
      stability     => required,
      require_level => hard
     }}).

-rabbit_feature_flag(
   {direct_exchange_routing_v2,
    #{desc          => "v2 direct exchange routing implementation",
      stability     => required,
      require_level => hard,
      depends_on    => [feature_flags_v2, implicit_default_bindings]
     }}).

-rabbit_feature_flag(
   {listener_records_in_ets,
    #{desc          => "Store listener records in ETS instead of Mnesia",
      stability     => required,
      require_level => hard,
      depends_on    => [feature_flags_v2]
     }}).

-rabbit_feature_flag(
   {tracking_records_in_ets,
    #{desc          => "Store tracking records in ETS instead of Mnesia",
      stability     => required,
      require_level => hard,
      depends_on    => [feature_flags_v2]
     }}).

-rabbit_feature_flag(
   {classic_queue_type_delivery_support,
    #{desc          => "Bug fix for classic queue deliveries using mixed versions",
      doc_url       => "https://github.com/rabbitmq/rabbitmq-server/issues/5931",
      %%TODO remove compatibility code
      stability     => required,
      require_level => hard,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
   {restart_streams,
    #{desc          => "Support for restarting streams with optional preferred next leader argument."
      "Used to implement stream leader rebalancing",
      stability     => required,
      require_level => hard,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
   {stream_sac_coordinator_unblock_group,
    #{desc          => "Bug fix to unblock a group of consumers in a super stream partition",
      doc_url       => "https://github.com/rabbitmq/rabbitmq-server/issues/7743",
      stability     => required,
      require_level => hard,
      depends_on    => [stream_single_active_consumer]
     }}).

-rabbit_feature_flag(
   {stream_filtering,
    #{desc          => "Support for stream filtering.",
      stability     => required,
      require_level => hard,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
   {message_containers,
    #{desc          => "Message containers.",
      stability     => required,
      require_level => hard,
      depends_on    => [feature_flags_v2]
     }}).

-rabbit_feature_flag(
   {khepri_db,
    #{desc          => "New Raft-based metadata store.",
      doc_url       => "https://www.rabbitmq.com/docs/next/metadata-store",
      stability     => stable,
      depends_on    => [feature_flags_v2,
                        direct_exchange_routing_v2,
                        maintenance_mode_status,
                        user_limits,
                        virtual_host_metadata,
                        tracking_records_in_ets,
                        listener_records_in_ets,

                        %% Deprecated features.
                        classic_queue_mirroring,
                        ram_node_type],
      callbacks     => #{enable =>
                         {rabbit_khepri, khepri_db_migration_enable},
                         post_enable =>
                         {rabbit_khepri, khepri_db_migration_post_enable}}
     }}).

-rabbit_feature_flag(
   {stream_update_config_command,
    #{desc          => "A new internal command that is used to update streams as "
                        "part of a policy.",
      stability     => required,
      require_level => hard,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
   {quorum_queue_non_voters,
    #{desc =>
          "Allows new quorum queue members to be added as non voters initially.",
      stability => stable,
      depends_on => [quorum_queue]
     }}).

-rabbit_feature_flag(
   {message_containers_deaths_v2,
    #{desc          => "Bug fix for dead letter cycle detection",
      doc_url       => "https://github.com/rabbitmq/rabbitmq-server/issues/11159",
      stability     => stable,
      depends_on    => [message_containers]
     }}).

%% We bundle the following separate concerns (which could have been separate feature flags)
%% into a single feature flag for better user experience:
%% 1. credit API v2 between classic / quorum queue client and classic / quorum queue server
%% 2. cancel API v2 betweeen classic queue client and classic queue server
%% 3. more compact quorum queue commands in quorum queue v4
%% 4. store messages in message containers AMQP 1.0 disk format v1
%% 5. support queue leader locator in classic queues
-rabbit_feature_flag(
   {'rabbitmq_4.0.0',
    #{desc          => "Allows rolling upgrades from 3.13.x to 4.0.x",
      stability     => stable,
      depends_on    => [message_containers]
     }}).

-rabbit_feature_flag(
   {'rabbitmq_4.1.0',
    #{desc          => "Allows rolling upgrades to 4.1.x",
      stability     => stable,
      depends_on    => ['rabbitmq_4.0.0']
     }}).

-rabbit_feature_flag(
   {'rabbitmq_4.2.0',
    #{desc          => "Allows rolling upgrades to 4.2.x",
      stability     => stable,
      depends_on    => ['rabbitmq_4.1.0']
     }}).
