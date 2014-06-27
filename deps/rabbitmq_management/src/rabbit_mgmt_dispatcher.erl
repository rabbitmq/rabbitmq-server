%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_dispatcher).

-export([modules/1, build_dispatcher/1]).

-behaviour(rabbit_mgmt_extension).
-export([dispatcher/0, web_ui/0]).

build_dispatcher(Ignore) ->
    [{["api" | Path], Mod, Args} ||
        {Path, Mod, Args} <-
            lists:append([Module:dispatcher() || Module <- modules(Ignore)])].

modules(IgnoreApps) ->
    [Module || {App, Module, Behaviours} <-
                   rabbit_misc:all_module_attributes(behaviour),
               not lists:member(App, IgnoreApps),
               lists:member(rabbit_mgmt_extension, Behaviours)].

%%----------------------------------------------------------------------------

web_ui()     -> [{javascript, <<"dispatcher.js">>}].

dispatcher() ->
    [{["overview"],                                                rabbit_mgmt_wm_overview, []},
     {["cluster-name"],                                            rabbit_mgmt_wm_cluster_name, []},
     {["nodes"],                                                   rabbit_mgmt_wm_nodes, []},
     {["nodes", node],                                             rabbit_mgmt_wm_node, []},
     {["extensions"],                                              rabbit_mgmt_wm_extensions, []},
     {["all-configuration"],                                       rabbit_mgmt_wm_definitions, []}, %% This was the old name, let's not break things gratuitously.
     {["definitions"],                                             rabbit_mgmt_wm_definitions, []},
     {["parameters"],                                              rabbit_mgmt_wm_parameters, []},
     {["parameters", component],                                   rabbit_mgmt_wm_parameters, []},
     {["parameters", component, vhost],                            rabbit_mgmt_wm_parameters, []},
     {["parameters", component, vhost, name],                      rabbit_mgmt_wm_parameter, []},
     {["policies"],                                                rabbit_mgmt_wm_policies, []},
     {["policies", vhost],                                         rabbit_mgmt_wm_policies, []},
     {["policies", vhost, name],                                   rabbit_mgmt_wm_policy, []},
     {["connections"],                                             rabbit_mgmt_wm_connections, []},
     {["connections", connection],                                 rabbit_mgmt_wm_connection, []},
     {["connections", connection, "channels"],                     rabbit_mgmt_wm_connection_channels, []},
     {["channels"],                                                rabbit_mgmt_wm_channels, []},
     {["channels", channel],                                       rabbit_mgmt_wm_channel, []},
     {["consumers"],                                               rabbit_mgmt_wm_consumers, []},
     {["consumers", vhost],                                        rabbit_mgmt_wm_consumers, []},
     {["exchanges"],                                               rabbit_mgmt_wm_exchanges, []},
     {["exchanges", vhost],                                        rabbit_mgmt_wm_exchanges, []},
     {["exchanges", vhost, exchange],                              rabbit_mgmt_wm_exchange, []},
     {["exchanges", vhost, exchange, "publish"],                   rabbit_mgmt_wm_exchange_publish, []},
     {["exchanges", vhost, exchange, "bindings", "source"],        rabbit_mgmt_wm_bindings, [exchange_source]},
     {["exchanges", vhost, exchange, "bindings", "destination"],   rabbit_mgmt_wm_bindings, [exchange_destination]},
     {["queues"],                                                  rabbit_mgmt_wm_queues, []},
     {["queues", vhost],                                           rabbit_mgmt_wm_queues, []},
     {["queues", vhost, queue],                                    rabbit_mgmt_wm_queue, []},
     {["queues", vhost, destination, "bindings"],                  rabbit_mgmt_wm_bindings, [queue]},
     {["queues", vhost, queue, "contents"],                        rabbit_mgmt_wm_queue_purge, []},
     {["queues", vhost, queue, "get"],                             rabbit_mgmt_wm_queue_get, []},
     {["queues", vhost, queue, "actions"],                         rabbit_mgmt_wm_queue_actions, []},
     {["bindings"],                                                rabbit_mgmt_wm_bindings, [all]},
     {["bindings", vhost],                                         rabbit_mgmt_wm_bindings, [all]},
     {["bindings", vhost, "e", source, dtype, destination],        rabbit_mgmt_wm_bindings, [source_destination]},
     {["bindings", vhost, "e", source, dtype, destination, props], rabbit_mgmt_wm_binding, []},
     {["vhosts"],                                                  rabbit_mgmt_wm_vhosts, []},
     {["vhosts", vhost],                                           rabbit_mgmt_wm_vhost, []},
     {["vhosts", vhost, "permissions"],                            rabbit_mgmt_wm_permissions_vhost, []},
     {["users"],                                                   rabbit_mgmt_wm_users, []},
     {["users", user],                                             rabbit_mgmt_wm_user, []},
     {["users", user, "permissions"],                              rabbit_mgmt_wm_permissions_user, []},
     {["whoami"],                                                  rabbit_mgmt_wm_whoami, []},
     {["permissions"],                                             rabbit_mgmt_wm_permissions, []},
     {["permissions", vhost, user],                                rabbit_mgmt_wm_permission, []},
     {["aliveness-test", vhost],                                   rabbit_mgmt_wm_aliveness_test, []}
    ].
