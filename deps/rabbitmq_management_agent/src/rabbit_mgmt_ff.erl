%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_ff).

-rabbit_feature_flag(
   {empty_basic_get_metric,
    #{desc          => "Count AMQP `basic.get` on empty queues in stats",
<<<<<<< HEAD
      stability     => required
     }}).

-rabbit_feature_flag(
  {drop_unroutable_metric,
   #{desc          => "Count unroutable publishes to be dropped in stats",
     stability     => required
    }}).
=======
      stability     => required,
      require_level => hard
     }}).

-rabbit_feature_flag(
   {drop_unroutable_metric,
    #{desc          => "Count unroutable publishes to be dropped in stats",
      stability     => required,
      require_level => hard
     }}).
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)
