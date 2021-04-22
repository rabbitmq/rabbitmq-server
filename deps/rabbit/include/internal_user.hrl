%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(is_internal_user(V),
        (?is_internal_user_v2(V) orelse
         ?is_internal_user_v1(V))).

-define(is_internal_user_v1(V), is_record(V, internal_user, 5)).
-define(is_internal_user_v2(V), is_record(V, internal_user, 6)).
