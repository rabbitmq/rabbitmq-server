%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-define(is_vhost_v1(V), is_record(V, vhost, 3)).

-define(vhost_v1_field_name(V), element(2, V)).
-define(vhost_v1_field_limits(V), element(3, V)).
