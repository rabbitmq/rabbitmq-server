%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-include("vhost_v1.hrl").
-include("vhost_v2.hrl").

-define(is_vhost(V),
        (?is_vhost_v2(V) orelse
         ?is_vhost_v1(V))).
