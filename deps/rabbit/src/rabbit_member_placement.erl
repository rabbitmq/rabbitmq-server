%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_member_placement).

%% Behaviour for pluggable quorum queue member placement strategies.
%%
%% A placement module is selected via the `member_placement_module` application
%% environment key (default: `rabbit_member_placement_az`). It is invoked when a
%% `member-placement-tag` is configured on the queue, policy, or globally.

-callback select_members(
            Size                  :: pos_integer(),
            AllNodes              :: [node(), ...],
            RunningNodes          :: [node()],
            QueueCount            :: non_neg_integer(),
            QueueCountStartRandom :: non_neg_integer(),
            GetQueues             :: function(),
            TagKey                :: binary()) -> {[node(), ...], function()}.
