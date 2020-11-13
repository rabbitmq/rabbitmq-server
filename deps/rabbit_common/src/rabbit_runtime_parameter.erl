%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
