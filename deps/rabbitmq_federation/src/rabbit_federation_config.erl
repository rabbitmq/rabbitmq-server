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

-module(rabbit_federation_config).
-behaviour(rabbit_cluster_config_item).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([validate/2, notify/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "federation config"},
                    {mfa, {rabbit_registry, register,
                           [cluster_config, <<"federation">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

validate(upstream_sets, Term) ->
    io:format("Validate upstream_sets ~p~n", [Term]),
    ok;

validate(connections, Term) ->
    io:format("Validate connections ~p~n", [Term]),
    ok;

validate(local_nodename, Term) ->
    io:format("Validate local_nodename ~p~n", [Term]),
    ok;

validate(local_username, Term) ->
    io:format("Validate local_username ~p~n", [Term]),
    ok;

validate(_Key, _Term) ->
    exit({error, key_not_recognised}).

notify(upstream_sets, Term) ->
    io:format("Notify upstream_sets ~p~n", [Term]),
    restart_everything(),
    ok;

notify(connections, Term) ->
    io:format("Notify connections ~p~n", [Term]),
    restart_everything(),
    ok;

notify(local_nodename, Term) ->
    io:format("Notify local_nodename ~p~n", [Term]),
    restart_everything(),
    ok;

notify(local_username, Term) ->
    io:format("Notify local_username ~p~n", [Term]),
    restart_everything(),
    ok.

%%----------------------------------------------------------------------------

%% TODO (maybe) it's a bit crude to just restart everything whenever
%% anything changes. Could we be cleaner?

restart_everything() ->
    Xs = lists:append([rabbit_exchange:list(VHost) ||
                          VHost <- rabbit_vhost:list()]),
    [rabbit_federation_sup:restart_child(XName) ||
        #exchange{name = XName,
                  type = 'x-federation'} <- Xs].
