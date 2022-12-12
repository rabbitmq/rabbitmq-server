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
-define(MQTT_GUIDE_URL, <<"https://rabbitmq.com/mqtt.html">>).
-define(MQTT_PROTO_V3, mqtt310).
-define(MQTT_PROTO_V4, mqtt311).
-define(INFO_ITEMS,
        [protocol,
         host,
         port,
         peer_host,
         peer_port,
         connection,
         conn_name,
         connection_state,
         ssl,
         ssl_protocol,
         ssl_key_exchange,
         ssl_cipher,
         ssl_hash,
         ssl_login_name,
         client_id,
         vhost,
         user,
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
