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
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_parameters).

-behaviour(rabbit_runtime_parameter).

-include("rabbit.hrl").

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
