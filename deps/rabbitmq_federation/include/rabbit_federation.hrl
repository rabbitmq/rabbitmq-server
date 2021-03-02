%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-record(upstream, {uris,
                   exchange_name,
                   queue_name,
                   consumer_tag,
                   prefetch_count,
                   max_hops,
                   reconnect_delay,
                   expires,
                   message_ttl,
                   trust_user_id,
                   ack_mode,
                   ha_policy,
                   name,
                   bind_nowait,
                   resource_cleanup_mode,
                   channel_use_mode
    }).

-record(upstream_params,
        {uri,
         params,
         x_or_q,
         %% The next two can be derived from the above three, but we don't
         %% want to do that every time we forward a message.
         safe_uri,
         table}).

%% Name of the message header used to collect the hop (forwarding) path
%% metadata as the message is forwarded by exchange federation.
-define(ROUTING_HEADER, <<"x-received-from">>).
-define(BINDING_HEADER, <<"x-bound-from">>).
-define(MAX_HOPS_ARG,   <<"x-max-hops">>).
%% Identifies a cluster, used by exchange federation cycle detection
-define(DOWNSTREAM_NAME_ARG,  <<"x-downstream-name">>).
%% Identifies a virtual host, used by exchange federation cycle detection
-define(DOWNSTREAM_VHOST_ARG, <<"x-downstream-vhost">>).
-define(DEF_PREFETCH, 1000).

-define(FEDERATION_GUIDE_URL, <<"https://rabbitmq.com/federation.html">>).

-define(FEDERATION_PG_SCOPE, rabbitmq_federation_pg_scope).