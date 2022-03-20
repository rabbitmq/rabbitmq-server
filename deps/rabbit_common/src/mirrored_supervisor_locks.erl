 %% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(mirrored_supervisor_locks).

-export([lock/1, unlock/1]).

 -define(KEY_PREFIX, mirrored_supervisor).

%%
%% API
%%

lock(Group) ->
    Nodes   = nodes(),
    %% about 300s, same as rabbit_nodes:lock_retries/0 default
    LockId  = case global:set_lock({?KEY_PREFIX, Group}, Nodes, 80) of
        true  -> Group;
        false -> undefined
    end,
    LockId.

unlock(LockId) ->
    Nodes = nodes(),
    case LockId of
        undefined -> ok;
        Value     -> global:del_lock({?KEY_PREFIX, Value}, Nodes)
    end,
    ok.
