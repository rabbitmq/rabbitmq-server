%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_runtime_parameter).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

-type(validate_results() ::
        'ok' | {error, string(), [term()]} | [validate_results()]).

-callback validate(rabbit_types:vhost(), binary(), binary(),
                   term(), rabbit_types:user()) -> validate_results().
-callback notify(rabbit_types:vhost(), binary(), binary(), term(),
                 rabbit_types:username()) -> 'ok'.
-callback notify_clear(rabbit_types:vhost(), binary(), binary(),
                       rabbit_types:username()) -> 'ok'.

added_to_rabbit_registry(_Type, _ModuleName) -> ok.
removed_from_rabbit_registry(_Type) -> ok.
