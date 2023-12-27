%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% A client ID that is tracked in Ra is a list of bytes
%% as returned by binary_to_list/1 in
%% https://github.com/rabbitmq/rabbitmq-server/blob/48467d6e1283b8d81e52cfd49c06ea4eaa31617d/deps/rabbitmq_mqtt/src/rabbit_mqtt_frame.erl#L137
%% prior to 3.12.0.
%% This has two downsides:
%% 1. Lists consume more memory than binaries (when tracking many clients).
%% 2. This violates the MQTT spec which states
%%    "The ClientId MUST be a UTF-8 encoded string as defined in Section 1.5.3 [MQTT-3.1.3-4]." [v4 3.1.3.1]
%% However, for backwards compatibility, we leave the client ID as a list of bytes in the Ra machine state because
%% feature flag delete_ra_cluster_mqtt_node introduced in 3.12.0 will delete the Ra cluster anyway.
-type client_id_ra() :: [byte()].

-record(machine_state, {
          client_ids = #{} :: #{client_id_ra() => Connection :: pid()},
          pids = #{} :: #{Connection :: pid() => [client_id_ra(), ...]},
          %% add acouple of fields for future extensibility
          reserved_1,
          reserved_2}).
