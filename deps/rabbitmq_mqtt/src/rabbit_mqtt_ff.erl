%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_ff).

-include("rabbit_mqtt.hrl").

-export([track_client_id_in_ra/0]).

-rabbit_feature_flag(
   {?QUEUE_TYPE_QOS_0,
    #{desc          => "Support pseudo queue type for MQTT QoS 0 subscribers omitting a queue process",
      stability     => stable
     }}).

-rabbit_feature_flag(
   {delete_ra_cluster_mqtt_node,
    #{desc          => "Delete Ra cluster 'mqtt_node' since MQTT client IDs are tracked locally",
      stability     => stable,
      callbacks     => #{enable => {mqtt_node, delete}}
     }}).

%% This feature flag is needed to prevent clients from downgrading an MQTT v5 session to v3 or v4 on
%% a lower version node. Such a session downgrade (or upgrade) requires changing binding arguments.
-rabbit_feature_flag(
   {mqtt_v5,
    #{desc          => "Support MQTT 5.0",
      stability     => stable
     }}).

-spec track_client_id_in_ra() -> boolean().
track_client_id_in_ra() ->
    rabbit_feature_flags:is_disabled(delete_ra_cluster_mqtt_node).
