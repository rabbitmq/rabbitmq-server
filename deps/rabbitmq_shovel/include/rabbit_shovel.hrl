%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-record(endpoint,
        {uris,
         resource_declaration
        }).

-record(shovel,
        {sources,
         destinations,
         prefetch_count,
         ack_mode,
         publish_fields,
         publish_properties,
         queue,
         reconnect_delay,
         delete_after = never
        }).

-define(SHOVEL_USER, <<"rmq-shovel">>).

-define(DEFAULT_PREFETCH, 1000).
-define(DEFAULT_ACK_MODE, on_confirm).
-define(DEFAULT_RECONNECT_DELAY, 5).

-define(SHOVEL_GUIDE_URL, <<"https://rabbitmq.com/shovel.html">>).
