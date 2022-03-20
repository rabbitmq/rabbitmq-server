%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_locks).

-export([lock/1, unlock/1]).

%%
%% API
%%

lock(Name) ->
    Nodes   = rabbit_nodes:all_running(),
    Retries = rabbit_nodes:lock_retries(),
    %% try to acquire a lock to avoid duplicate starts
    LockId = case global:set_lock({dynamic_shovel, Name}, Nodes, Retries) of
        true  -> Name;
        false -> undefined
    end,
    LockId.

unlock(LockId) ->
    Nodes = rabbit_nodes:all_running(),
    case LockId of
        undefined -> ok;
        Value     -> global:del_lock({dynamic_shovel, Value}, Nodes)
    end,
    ok.
