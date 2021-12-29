%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_dispatcher).

-export([modules/1, build_dispatcher/1]).

-behaviour(rabbit_mgmt_extension).
-export([dispatcher/0, web_ui/0]).

build_dispatcher(Ignore) ->
    Routes = build_routes(Ignore),
    cowboy_router:compile(Routes).

build_routes(Ignore) ->
    ManagementApp = module_app(?MODULE),
    Prefix = rabbit_mgmt_util:get_path_prefix(),
    RootIdxRtes = build_root_index_routes(Prefix, ManagementApp),
    ApiRdrRte = build_redirect_route("/api", Prefix ++ "/api/index.html"),
    CliRdrRte = build_redirect_route("/cli", Prefix ++ "/cli/index.html"),
    StatsRdrRte1 = build_redirect_route("/stats", Prefix ++ "/api/index.html"),
    StatsRdrRte2 = build_redirect_route("/doc/stats.html", Prefix ++ "/api/index.html"),
    MgmtRdrRte = {"/mgmt", rabbit_mgmt_wm_redirect, "/"},
    LocalPaths = [{module_app(M), "www"} || M <- modules(Ignore)],
    LocalStaticRte = {"/[...]", rabbit_mgmt_wm_static, LocalPaths},
    % NB: order is significant in the routing list
    Routes0 = build_module_routes(Ignore) ++
        [ApiRdrRte, CliRdrRte, MgmtRdrRte, StatsRdrRte1, StatsRdrRte2, LocalStaticRte],
    Routes1 = maybe_add_path_prefix(Routes0, Prefix),
    % NB: ensure the root routes are first
    Routes2 = RootIdxRtes ++ Routes1,
    [{'_', Routes2}].

build_root_index_routes("", ManagementApp) ->
    [{"/", rabbit_mgmt_wm_static, root_idx_file(ManagementApp)}];
build_root_index_routes(Prefix, ManagementApp) ->
    [{"/", rabbit_mgmt_wm_redirect, Prefix ++ "/"},
     {Prefix, rabbit_mgmt_wm_static, root_idx_file(ManagementApp)}].

build_redirect_route(Path, Location) ->
    {Path, rabbit_mgmt_wm_redirect, Location}.

root_idx_file(ManagementApp) ->
    {priv_file, ManagementApp, "www/index.html"}.

maybe_add_path_prefix(Routes, "") ->
    Routes;
maybe_add_path_prefix(Routes, Prefix) ->
    [{Prefix ++ Path, Mod, Args} || {Path, Mod, Args} <- Routes].

build_module_routes(Ignore) ->
    Routes = [Module:dispatcher() || Module <- modules(Ignore)],
    [{"/api" ++ Path, Mod, Args} || {Path, Mod, Args} <- lists:append(Routes)].

modules(IgnoreApps) ->
    [Module || {App, Module, Behaviours} <-
               %% Sort rabbitmq_management modules first. This is
               %% a microoptimization because most files belong to
               %% this application. Making it first avoids several
               %% stats(2) which have a great chance of failing in other
               %% applications.
               lists:sort(
                 fun
                     ({rabbitmq_management, _, _}, _) -> true;
                     (_, {rabbitmq_management, _, _}) -> false;
                     ({A, _, _}, {B, _, _})           -> A =< B
                 end,
                 rabbit_misc:all_module_attributes(behaviour)),
               not lists:member(App, IgnoreApps),
               lists:member(rabbit_mgmt_extension, Behaviours)].

module_app(Module) ->
    {ok, App} = application:get_application(Module),
    App.

%%----------------------------------------------------------------------------

web_ui()     -> [{javascript, <<"dispatcher.js">>}].

dispatcher() ->
    [{"/overview",                                             rabbit_mgmt_wm_overview, []},
     {"/cluster-name",                                         rabbit_mgmt_wm_cluster_name, []},
     {"/nodes",                                                rabbit_mgmt_wm_nodes, []},
     {"/nodes/:node",                                          rabbit_mgmt_wm_node, []},
     {"/nodes/:node/memory",                                   rabbit_mgmt_wm_node_memory, [absolute]},
     {"/nodes/:node/memory/relative",                          rabbit_mgmt_wm_node_memory, [relative]},
     {"/nodes/:node/memory/ets",                               rabbit_mgmt_wm_node_memory_ets, [absolute]},
     {"/nodes/:node/memory/ets/relative",                      rabbit_mgmt_wm_node_memory_ets, [relative]},
     {"/nodes/:node/memory/ets/:filter",                       rabbit_mgmt_wm_node_memory_ets, [absolute]},
     {"/nodes/:node/memory/ets/:filter/relative",              rabbit_mgmt_wm_node_memory_ets, [relative]},
     {"/extensions",                                           rabbit_mgmt_wm_extensions, []},
     {"/all-configuration",                                    rabbit_mgmt_wm_definitions, []}, %% This was the old name, let's not break things gratuitously.
     {"/definitions",                                          rabbit_mgmt_wm_definitions, []},
     {"/definitions/:vhost",                                   rabbit_mgmt_wm_definitions, []},
     {"/parameters",                                           rabbit_mgmt_wm_parameters, []},
     {"/parameters/:component",                                rabbit_mgmt_wm_parameters, []},
     {"/parameters/:component/:vhost",                         rabbit_mgmt_wm_parameters, []},
     {"/parameters/:component/:vhost/:name",                   rabbit_mgmt_wm_parameter, []},
     {"/global-parameters",                                    rabbit_mgmt_wm_global_parameters, []},
     {"/global-parameters/:name",                              rabbit_mgmt_wm_global_parameter, []},
     {"/policies",                                             rabbit_mgmt_wm_policies, []},
     {"/policies/:vhost",                                      rabbit_mgmt_wm_policies, []},
     {"/policies/:vhost/:name",                                rabbit_mgmt_wm_policy, []},
     {"/operator-policies",                                    rabbit_mgmt_wm_operator_policies, []},
     {"/operator-policies/:vhost",                             rabbit_mgmt_wm_operator_policies, []},
     {"/operator-policies",                                    rabbit_mgmt_wm_operator_policies, []},
     {"/operator-policies/:vhost/:name",                       rabbit_mgmt_wm_operator_policy, []},
     {"/vhost-limits/:vhost/:name",                            rabbit_mgmt_wm_limit, []},
     {"/vhost-limits",                                         rabbit_mgmt_wm_limits, []},
     {"/vhost-limits/:vhost",                                  rabbit_mgmt_wm_limits, []},
     {"/connections",                                          rabbit_mgmt_wm_connections, []},
     {"/connections/:connection",                              rabbit_mgmt_wm_connection, []},
     {"/connections/:connection/channels",                     rabbit_mgmt_wm_connection_channels, []},
     {"/channels",                                             rabbit_mgmt_wm_channels, []},
     {"/channels/:channel",                                    rabbit_mgmt_wm_channel, []},
     {"/consumers",                                            rabbit_mgmt_wm_consumers, []},
     {"/consumers/:vhost",                                     rabbit_mgmt_wm_consumers, []},
     {"/exchanges",                                            rabbit_mgmt_wm_exchanges, []},
     {"/exchanges/:vhost",                                     rabbit_mgmt_wm_exchanges, []},
     {"/exchanges/:vhost/:exchange",                           rabbit_mgmt_wm_exchange, []},
     {"/exchanges/:vhost/:exchange/publish",                   rabbit_mgmt_wm_exchange_publish, []},
     {"/exchanges/:vhost/:exchange/bindings/source",           rabbit_mgmt_wm_bindings, [exchange_source]},
     {"/exchanges/:vhost/:exchange/bindings/destination",      rabbit_mgmt_wm_bindings, [exchange_destination]},
     {"/queues",                                               rabbit_mgmt_wm_queues, []},
     {"/queues/:vhost",                                        rabbit_mgmt_wm_queues, []},
     {"/queues/:vhost/:queue",                                 rabbit_mgmt_wm_queue, []},
     {"/queues/:vhost/:destination/bindings",                  rabbit_mgmt_wm_bindings, [queue]},
     {"/queues/:vhost/:queue/contents",                        rabbit_mgmt_wm_queue_purge, []},
     {"/queues/:vhost/:queue/get",                             rabbit_mgmt_wm_queue_get, []},
     {"/queues/:vhost/:queue/actions",                         rabbit_mgmt_wm_queue_actions, []},
     {"/bindings",                                             rabbit_mgmt_wm_bindings, [all]},
     {"/bindings/:vhost",                                      rabbit_mgmt_wm_bindings, [all]},
     {"/bindings/:vhost/e/:source/:dtype/:destination",        rabbit_mgmt_wm_bindings, [source_destination]},
     {"/bindings/:vhost/e/:source/:dtype/:destination/:props", rabbit_mgmt_wm_binding, []},
     {"/vhosts",                                               rabbit_mgmt_wm_vhosts, []},
     {"/vhosts/:vhost",                                        rabbit_mgmt_wm_vhost, []},
     {"/vhosts/:vhost/start/:node",                            rabbit_mgmt_wm_vhost_restart, []},
     {"/vhosts/:vhost/permissions",                            rabbit_mgmt_wm_permissions_vhost, []},
     {"/vhosts/:vhost/topic-permissions",                      rabbit_mgmt_wm_topic_permissions_vhost, []},
     %% /connections/:connection is already taken, we cannot use our standard scheme here
     {"/vhosts/:vhost/connections",                            rabbit_mgmt_wm_connections_vhost, []},
     %% /channels/:channel is already taken, we cannot use our standard scheme here
     {"/vhosts/:vhost/channels",                               rabbit_mgmt_wm_channels_vhost, []},
     {"/users/bulk-delete",                                    rabbit_mgmt_wm_users_bulk_delete, []},
     {"/users/without-permissions",                            rabbit_mgmt_wm_users, [without_permissions]},
     {"/users",                                                rabbit_mgmt_wm_users, [all]},
     {"/users/:user",                                          rabbit_mgmt_wm_user, []},
     {"/users/:user/permissions",                              rabbit_mgmt_wm_permissions_user, []},
     {"/users/:user/topic-permissions",                        rabbit_mgmt_wm_topic_permissions_user, []},
     {"/user-limits/:user/:name",                              rabbit_mgmt_wm_user_limit, []},
     {"/user-limits",                                          rabbit_mgmt_wm_user_limits, []},
     {"/user-limits/:user",                                    rabbit_mgmt_wm_user_limits, []},
     {"/feature-flags",                                        rabbit_mgmt_wm_feature_flags, []},
     {"/feature-flags/:name/enable",                           rabbit_mgmt_wm_feature_flag_enable, []},
     {"/whoami",                                               rabbit_mgmt_wm_whoami, []},
     {"/permissions",                                          rabbit_mgmt_wm_permissions, []},
     {"/permissions/:vhost/:user",                             rabbit_mgmt_wm_permission, []},
     {"/topic-permissions",                                    rabbit_mgmt_wm_topic_permissions, []},
     {"/topic-permissions/:vhost/:user",                       rabbit_mgmt_wm_topic_permission, []},
     {"/topic-permissions/:vhost/:user/:exchange",             rabbit_mgmt_wm_topic_permission, []},
     {"/aliveness-test/:vhost",                                rabbit_mgmt_wm_aliveness_test, []},
     %% now deprecated
     {"/healthchecks/node",                                    rabbit_mgmt_wm_healthchecks, []},
     {"/healthchecks/node/:node",                              rabbit_mgmt_wm_healthchecks, []},
     %% modern generation of fine-grained health checks
     {"/health/checks/alarms",                                 rabbit_mgmt_wm_health_check_alarms, []},
     {"/health/checks/local-alarms",                           rabbit_mgmt_wm_health_check_local_alarms, []},
     {"/health/checks/certificate-expiration/:within/:unit",   rabbit_mgmt_wm_health_check_certificate_expiration, []},
     {"/health/checks/port-listener/:port",                    rabbit_mgmt_wm_health_check_port_listener, []},
     {"/health/checks/protocol-listener/:protocol",            rabbit_mgmt_wm_health_check_protocol_listener, []},
     {"/health/checks/virtual-hosts",                          rabbit_mgmt_wm_health_check_virtual_hosts, []},
     {"/health/checks/node-is-mirror-sync-critical",           rabbit_mgmt_wm_health_check_node_is_mirror_sync_critical, []},
     {"/health/checks/node-is-quorum-critical",                rabbit_mgmt_wm_health_check_node_is_quorum_critical, []},
     {"/reset",                                                rabbit_mgmt_wm_reset, []},
     {"/reset/:node",                                          rabbit_mgmt_wm_reset, []},
     {"/rebalance/queues",                                     rabbit_mgmt_wm_rebalance_queues, [{queues, all}]},
     {"/auth",                                                 rabbit_mgmt_wm_auth, []},
     {"/auth/attempts/:node",                                  rabbit_mgmt_wm_auth_attempts, [all]},
     {"/auth/attempts/:node/source",                           rabbit_mgmt_wm_auth_attempts, [by_source]},
     {"/login",                                                rabbit_mgmt_wm_login, []}
    ].
