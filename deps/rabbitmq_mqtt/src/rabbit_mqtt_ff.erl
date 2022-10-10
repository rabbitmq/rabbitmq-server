%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_ff).

-export([track_client_id_in_ra/0]).

-rabbit_feature_flag(
   {delete_ra_cluster_mqtt_node,
    #{desc          => "Delete Ra cluster 'mqtt_node' because from now on MQTT client IDs are tracked locally",
      stability     => stable,
      callbacks     => #{enable => {mqtt_node, delete}}
     }}).

track_client_id_in_ra() ->
    not rabbit_feature_flags:is_enabled(delete_ra_cluster_mqtt_node).
