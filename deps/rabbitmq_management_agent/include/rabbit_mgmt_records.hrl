%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-include_lib("rabbitmq_web_dispatch/include/rabbitmq_web_dispatch_records.hrl").

-record(range, {first :: integer(),
                last  :: integer(),
                incr  :: integer()}).
