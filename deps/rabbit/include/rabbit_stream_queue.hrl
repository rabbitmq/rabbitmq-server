%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Osiris keeps offsets in signed 64-bit counters. Half that range leaves a
%% stream starting at the maximum room for as many messages again.
-define(MAX_STREAM_INITIAL_OFFSET, ((1 bsl 62) - 1)).
