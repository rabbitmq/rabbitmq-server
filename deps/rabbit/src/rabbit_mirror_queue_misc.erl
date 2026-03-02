%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mirror_queue_misc).

%% Deprecated feature callback.
-export([are_cmqs_used/1]).

-rabbit_deprecated_feature(
   {classic_queue_mirroring,
    #{deprecation_phase => removed,
      messages =>
      #{when_permitted =>
        "Classic mirrored queues are deprecated.\n"
        "By default, they can still be used for now.\n"
        "Their use will not be permitted by default in the next minor"
        "RabbitMQ version (if any) and they will be removed from "
        "RabbitMQ 4.0.0.\n"
        "To continue using classic mirrored queues when they are not "
        "permitted by default, set the following parameter in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = true\"\n"
        "To test RabbitMQ as if they were removed, set this in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = false\"",

        when_denied =>
        "Classic mirrored queues are deprecated.\n"
        "Their use is not permitted per the configuration (overriding the "
        "default, which is permitted):\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = false\"\n"
        "Their use will not be permitted by default in the next minor "
        "RabbitMQ version (if any) and they will be removed from "
        "RabbitMQ 4.0.0.\n"
        "To continue using classic mirrored queues when they are not "
        "permitted by default, set the following parameter in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = true\"",

        when_removed =>
        "Classic mirrored queues have been removed.\n"
       },
      doc_url => "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#removal-of-classic-queue-mirroring",
      callbacks => #{is_feature_used => {?MODULE, are_cmqs_used}}
     }}).

%%----------------------------------------------------------------------------

are_cmqs_used(_) ->
    false.
