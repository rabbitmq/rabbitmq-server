%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_top_extension).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).

dispatcher() -> [{"/top/:node",    rabbit_top_wm_processes, []},
                 {"/top/ets/:node", rabbit_top_wm_ets_tables, []},
                 {"/process/:pid", rabbit_top_wm_process, []}].

web_ui()     -> [{javascript, <<"top.js">>}].
