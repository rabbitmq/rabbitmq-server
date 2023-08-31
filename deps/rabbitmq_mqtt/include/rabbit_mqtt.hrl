%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(APP_NAME, rabbitmq_mqtt).
-define(PG_SCOPE, pg_scope_rabbitmq_mqtt_clientid).
-define(QUEUE_TYPE_QOS_0, rabbit_mqtt_qos0_queue).
-define(PERSISTENT_TERM_MAILBOX_SOFT_LIMIT, mqtt_mailbox_soft_limit).
-define(PERSISTENT_TERM_EXCHANGE, mqtt_exchange).
-define(MQTT_GUIDE_URL, <<"https://rabbitmq.com/mqtt.html">>).

-define(MQTT_PROTO_V3, mqtt310).
-define(MQTT_PROTO_V4, mqtt311).
-define(MQTT_PROTO_V5, mqtt50).
-type protocol_version_atom() :: ?MQTT_PROTO_V3 | ?MQTT_PROTO_V4 | ?MQTT_PROTO_V5.

-define(ITEMS,
        [pid,
         protocol,
         host,
         port,
         peer_host,
         peer_port,
         ssl,
         ssl_protocol,
         ssl_key_exchange,
         ssl_cipher,
         ssl_hash,
         vhost,
         user
        ]).

-define(INFO_ITEMS,
        ?ITEMS ++
        [
         client_id,
         conn_name,
         user_property,
         connection_state,
         ssl_login_name,
         recv_cnt,
         recv_oct,
         send_cnt,
         send_oct,
         send_pend,
         clean_sess,
         will_msg,
         retainer_pid,
         exchange,
         prefetch,
         messages_unconfirmed,
         messages_unacknowledged
        ]).

%% Connection opened or closed.
-define(EVENT_KEYS,
        ?ITEMS ++
        [name,
         client_properties,
         peer_cert_issuer,
         peer_cert_subject,
         peer_cert_validity,
         auth_mechanism,
         timeout,
         frame_max,
         channel_max,
         connected_at,
         node,
         user_who_performed_action
        ]).

-define(SIMPLE_METRICS,
        [pid,
         recv_oct,
         send_oct,
         reductions]).
-define(OTHER_METRICS,
        [recv_cnt,
         send_cnt,
         send_pend,
         garbage_collection,
         state]).
