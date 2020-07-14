%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(QUEUE_PREFIX, "/queue").
-define(TOPIC_PREFIX, "/topic").
-define(EXCHANGE_PREFIX, "/exchange").
-define(AMQQUEUE_PREFIX, "/amq/queue").
-define(TEMP_QUEUE_PREFIX, "/temp-queue").
%% reply queues names can have slashes in the content so no further
%% parsing happens.
-define(REPLY_QUEUE_PREFIX, "/reply-queue/").
