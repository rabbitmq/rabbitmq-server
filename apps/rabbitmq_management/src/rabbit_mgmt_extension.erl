%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_extension).

%% Return a Cowboy dispatcher table to integrate
-callback dispatcher() -> [{string(), atom(), [atom()]}].

%% Return a proplist of information for the web UI to integrate
%% this extension. Currently the proplist should have one key,
%% 'javascript', the name of a javascript file to load and run.
-callback web_ui() -> proplists:proplist().
