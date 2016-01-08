%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_parameters).

-behaviour(rabbit_runtime_parameter).

-export([register/0]).
-export([validate/5, notify/4, notify_clear/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange parameters"},
                    {mfa, {rabbit_exchange_parameters, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    rabbit_registry:register(runtime_parameter,
                             <<"exchange-delete-in-progress">>, ?MODULE).

validate(_VHost, <<"exchange-delete-in-progress">>, _Name, _Term, _User) ->
    ok.

notify(_VHost, <<"exchange-delete-in-progress">>, _Name, _Term) ->
    ok.

notify_clear(_VHost, <<"exchange-delete-in-progress">>, _Name) ->
    ok.
