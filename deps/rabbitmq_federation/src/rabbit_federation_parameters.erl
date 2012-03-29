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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_parameters).
-behaviour(rabbit_runtime_parameter).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([validate/3, notify/3, notify_clear/2]).
-export([register/0]).

-rabbit_boot_step({?MODULE,
                   [{description, "federation parameters"},
                    {mfa, {rabbit_federation_parameters, register, []}},
                    {mfa, {rabbit_registry, register,
                           [runtime_parameter, <<"federation">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(runtime_parameter, Name, ?MODULE) ||
        Name <- [<<"federation">>,
                 <<"federation_connection">>,
                 <<"federation_upstream_set">>]].

validate(<<"federation_upstream_set">>, _Key, _Term) ->
    ok;

validate(<<"federation_connection">>, _Key, _Term) ->
    ok;

validate(<<"federation">>, <<"local_nodename">>, _Term) ->
    ok;

validate(<<"federation">>, <<"local_username">>, _Term) ->
    ok;

validate(_AppName, _Key, _Term) ->
    exit({error, key_not_recognised}).

notify(<<"federation_upstream_set">>, _Key, _Term) ->
    rabbit_federation_link_sup_sup:restart_everything();

notify(<<"federation_connection">>, _Key, _Term) ->
    rabbit_federation_link_sup_sup:restart_everything();

notify(<<"federation">>, <<"local_nodename">>, _Term) ->
    rabbit_federation_link_sup_sup:restart_everything();

notify(<<"federation">>, <<"local_username">>, _Term) ->
    rabbit_federation_link_sup_sup:restart_everything().

notify_clear(<<"federation_upstream_set">>, _Key) ->
    rabbit_federation_link_sup_sup:restart_everything();

notify_clear(<<"federation_connection">>, _Key) ->
    rabbit_federation_link_sup_sup:restart_everything();

notify_clear(<<"federation">>, <<"local_nodename">>) ->
    rabbit_federation_link_sup_sup:restart_everything();

notify_clear(<<"federation">>, <<"local_username">>) ->
    rabbit_federation_link_sup_sup:restart_everything().
