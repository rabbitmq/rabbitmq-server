%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-define(SIMPLE_METRICS, [pid,
                         recv_oct,
                         send_oct,
                         reductions]).

-define(OTHER_METRICS, [recv_cnt,
                        send_cnt,
                        send_pend,
                        state,
                        channels,
                        garbage_collection]).
