%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_parameters).

-behaviour(rabbit_runtime_parameter).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([register/0]).
-export([validate/5, notify/5, notify_clear/4]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange parameters"},
                    {mfa, {rabbit_exchange_parameters, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    rabbit_registry:register(runtime_parameter,
                             ?EXCHANGE_DELETE_IN_PROGRESS_COMPONENT, ?MODULE),
    %% ensure there are no leftovers from before node restart/crash
    rabbit_runtime_parameters:clear_component(
      ?EXCHANGE_DELETE_IN_PROGRESS_COMPONENT,
      ?INTERNAL_USER),
    ok.

validate(_VHost, ?EXCHANGE_DELETE_IN_PROGRESS_COMPONENT, _Name, _Term, _User) ->
    ok.

notify(_VHost, ?EXCHANGE_DELETE_IN_PROGRESS_COMPONENT, _Name, _Term, _Username) ->
    ok.

notify_clear(_VHost, ?EXCHANGE_DELETE_IN_PROGRESS_COMPONENT, _Name, _Username) ->
    ok.
