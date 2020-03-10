%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
%%        current slaves,
%%        current synchronised slaves,
%%        all nodes to consider
%%
%% Returns: tuple of new master, new slaves
%%
-callback suggested_queue_nodes(
            params(), master(), [slave()], [slave()], [node()]) ->
    {master(), [slave()]}.

%% Are the parameters valid for this mode?
-callback validate_policy(params()) ->
    rabbit_policy_validator:validate_results().

added_to_rabbit_registry(_Type, _ModuleName) -> ok.
removed_from_rabbit_registry(_Type) -> ok.
