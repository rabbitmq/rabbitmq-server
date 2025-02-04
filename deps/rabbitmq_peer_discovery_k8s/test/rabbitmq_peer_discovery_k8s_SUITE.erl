%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbitmq_peer_discovery_k8s_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("stdlib/include/assert.hrl").

all() ->
    [
     {group, unit}
    ].

groups() ->
    [
     {unit, [], [
                 returns_node_0_by_default,
                 ordinal_start_is_configurable,
                 seed_node_can_be_explicitly_configured
                ]}
    ].

returns_node_0_by_default(_Config) ->
    meck:new(rabbit_peer_discovery_k8s, [passthrough]),

    Cases = #{
              'rabbit@foo-server-0.foo-nodes.default' =>  {ok, {'rabbit@foo-server-0.foo-nodes.default', disc} },
              'rabbit@foo-server-10.foo-nodes.default' => {ok, {'rabbit@foo-server-0.foo-nodes.default', disc} },
              'rabbit@foo-0-bar-1.foo-0-bar-nodes.default' => {ok, {'rabbit@foo-0-bar-0.foo-0-bar-nodes.default', disc} },
              'rabbit@foo--0-bar--1.foo0.default' => {ok, {'rabbit@foo--0-bar--0.foo0.default', disc} },
              'bunny@hop' => {error, "my nodename (bunny@hop) doesn't seem to have the expected -ID suffix like StatefulSet pods should"}
             },

    [begin
         meck:expect(rabbit_peer_discovery_k8s, node, fun() -> Nodename end),
         ?assertEqual(Result, rabbitmq_peer_discovery_k8s:list_nodes())
     end || Nodename := Result <- Cases ],

    meck:unload([rabbit_peer_discovery_k8s]).

ordinal_start_is_configurable(_Config) ->
    meck:new(rabbit_peer_discovery_k8s, [passthrough]),

    application:set_env(rabbit, cluster_formation,
                        [
                         {peer_discovery_backend, rabbit_peer_discovery_k8s},
                         {peer_discovery_k8s, [
                                                {ordinal_start, 123}
                                              ]}
                        ]),

    Cases = #{
              'rabbit@foo-server-0.foo-nodes.default' =>  {ok, {'rabbit@foo-server-123.foo-nodes.default', disc} },
              'rabbit@foo-server-10.foo-nodes.default' => {ok, {'rabbit@foo-server-123.foo-nodes.default', disc} },
              'rabbit@foo-0-bar-1.foo-0-bar-nodes.default' => {ok, {'rabbit@foo-0-bar-123.foo-0-bar-nodes.default', disc} },
              'rabbit@foo--0-bar--1.foo0.default' => {ok, {'rabbit@foo--0-bar--123.foo0.default', disc} },
              'bunny@hop' => {error, "my nodename (bunny@hop) doesn't seem to have the expected -ID suffix like StatefulSet pods should"}
             },

    [begin
         meck:expect(rabbit_peer_discovery_k8s, node, fun() -> Nodename end),
         ?assertEqual(Result, rabbitmq_peer_discovery_k8s:list_nodes())
     end || Nodename := Result <- Cases ],

    application:unset_env(rabbit, cluster_formation),
    meck:unload([rabbit_peer_discovery_k8s]).

seed_node_can_be_explicitly_configured(_Config) ->
    meck:new(rabbit_peer_discovery_k8s, [passthrough]),

    application:set_env(rabbit, cluster_formation,
                        [
                         {peer_discovery_backend, rabbit_peer_discovery_k8s},
                         {peer_discovery_k8s, [
                                                {seed_node, "foo@seed-node"}
                                              ]}
                        ]),
    Cases = #{
              'rabbit@foo-server-0.foo-nodes.default' =>  {ok, {'foo@seed-node', disc} },
              'bunny@hop' =>  {ok, {'foo@seed-node', disc} }
             },

    [begin
         meck:expect(rabbit_peer_discovery_k8s, node, fun() -> Nodename end),
         ?assertEqual(Result, rabbitmq_peer_discovery_k8s:list_nodes())
     end || Nodename := Result <- Cases ],

    meck:unload([rabbit_peer_discovery_k8s]).
