%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-define(is_vhost_v2(V), is_record(V, vhost, 4)).

-define(vhost_v2_field_name(Q), element(2, Q)).
-define(vhost_v2_field_limits(Q), element(3, Q)).
-define(vhost_v2_field_metadata(Q), element(4, Q)).
