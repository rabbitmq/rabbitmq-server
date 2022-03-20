%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_ff).

-rabbit_feature_flag(
   {empty_basic_get_metric,
    #{desc          => "Count AMQP `basic.get` on empty queues in stats",
      stability     => stable
     }}).

-rabbit_feature_flag(
  {drop_unroutable_metric,
   #{desc          => "Count unroutable publishes to be dropped in stats",
     stability     => stable
    }}).
