%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-record(context, {user,
                  password = none,
                  impl}). % storage for a context of the resource handler

-record(range, {first :: integer(),
                last  :: integer(),
                incr  :: integer()}).


