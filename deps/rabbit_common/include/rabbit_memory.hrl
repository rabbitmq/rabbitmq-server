%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(DEFAULT_MEMORY_CHECK_INTERVAL, 1000).
-define(ONE_MiB, 1048576).

%% For an unknown OS, we assume that we have 1GB of memory. It'll be
%% wrong. Scale by vm_memory_high_watermark in configuration to get a
%% sensible value.
-define(MEMORY_SIZE_FOR_UNKNOWN_OS, 1073741824).
-define(DEFAULT_VM_MEMORY_HIGH_WATERMARK, 0.4).
-define(MAX_VM_MEMORY_HIGH_WATERMARK, 1.0).
