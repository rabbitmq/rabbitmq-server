%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_mode).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

-type master() :: node().
-type slave() :: node().
-type params() :: any().

-callback description() -> [proplists:property()].

%% Called whenever we think we might need to change nodes for a
%% mirrored queue. Note that this is called from a variety of
%% contexts, both inside and outside Mnesia transactions. Ideally it
%% will be pure-functional.
%%
%% Takes: parameters set in the policy,
%%        current master,
%%        current mirrors,
%%        current synchronised mirrors,
%%        all nodes to consider
%%
%% Returns: tuple of new master, new mirrors
%%
-callback suggested_queue_nodes(
            params(), master(), [slave()], [slave()], [node()]) ->
    {master(), [slave()]}.

%% Are the parameters valid for this mode?
-callback validate_policy(params()) ->
    rabbit_policy_validator:validate_results().

added_to_rabbit_registry(_Type, _ModuleName) -> ok.
removed_from_rabbit_registry(_Type) -> ok.
