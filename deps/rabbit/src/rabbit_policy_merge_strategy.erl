%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_policy_merge_strategy).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

-callback merge_policy_value(binary(), Value, Value) ->
    Value
    when Value :: term().

added_to_rabbit_registry(_Type, _ModuleName) -> ok.
removed_from_rabbit_registry(_Type) -> ok.
