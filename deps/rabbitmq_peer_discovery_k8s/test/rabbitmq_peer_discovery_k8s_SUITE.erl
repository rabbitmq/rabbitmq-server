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
                 returns_node_0
                ]}
    ].

returns_node_0(_Config) ->
    meck:new(rabbit_peer_discovery_k8s, [passthrough]),

    Cases = #{
              'rabbit@foo-server-0.foo-nodes.default' =>  {ok, {'rabbit@foo-server-0.foo-nodes.default', disc} },
              'rabbit@foo-server-10.foo-nodes.default' => {ok, {'rabbit@foo-server-0.foo-nodes.default', disc} },
              'rabbit@foo-0-bar-1.foo-0-bar-nodes.default' => {ok, {'rabbit@foo-0-bar-0.foo-0-bar-nodes.default', disc} },
              'rabbit@foo--0-bar--1.foo0.default' => {ok, {'rabbit@foo--0-bar--0.foo0.default', disc} },
              'bunny@hop' => {error, "my nodename (bunny@hop) doesn't seem to be have an -ID suffix like StatefulSet pods should"}
             },

    [begin
         meck:expect(rabbit_peer_discovery_k8s, node, fun() -> Nodename end),
         ?assertEqual(Result, rabbitmq_peer_discovery_k8s:list_nodes())
     end || Nodename := Result <- Cases ],

    meck:unload([rabbit_peer_discovery_k8s]).
