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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_test_clustering).

-compile(export_all).
-include("rabbit_mgmt_test.hrl").

-import(rabbit_mgmt_test_http, [http_get/1, http_put/3, http_delete/2]).
-import(rabbit_misc, [pget/2]).

%%----------------------------------------------------------------------------

cluster_nodes_with() -> cluster_ab.
cluster_nodes([_A, _B]) ->
    ?assertEqual(2, length(http_get("/nodes"))),
    ok.

ha_with() -> cluster_ab.
ha([RabbitCfg, HareCfg]) ->
    Rabbit = pget(nodename, RabbitCfg),
    Hare = pget(nodename, HareCfg),
    Policy = [{pattern,    <<".*">>},
              {definition, [{'ha-mode', <<"all">>}]}],
    http_put("/policies/%2f/HA", Policy, ?NO_CONTENT),
    QArgs = [{node, list_to_binary(atom_to_list(Hare))}],
    http_put("/queues/%2f/ha-queue", QArgs, ?NO_CONTENT),
    Q = wait_for("/queues/%2f/ha-queue"),
    assert_node(Hare, pget(node, Q)),
    assert_single_node(Rabbit, pget(slave_nodes, Q)),
    assert_single_node(Rabbit, pget(synchronised_slave_nodes, Q)),
    _HareCfg2 = rabbit_test_configs:restart_node(HareCfg),

    Q2 = wait_for("/queues/%2f/ha-queue"),
    assert_node(Rabbit, pget(node, Q2)),
    assert_single_node(Hare, pget(slave_nodes, Q2)),
    assert_single_node(Hare, pget(synchronised_slave_nodes, Q2)),
    http_delete("/queues/%2f/ha-queue", ?NO_CONTENT),
    http_delete("/policies/%2f/HA", ?NO_CONTENT),
    ok.

%%----------------------------------------------------------------------------

wait_for(Path) ->
    wait_for(Path, [slave_nodes, synchronised_slave_nodes]).

wait_for(Path, Keys) ->
    wait_for(Path, Keys, 1000).

wait_for(Path, Keys, 0) ->
    exit({timeout, {Path, Keys}});

wait_for(Path, Keys, Count) ->
    Res = http_get(Path),
    case present(Keys, Res) of
        false -> timer:sleep(10),
                 wait_for(Path, Keys, Count - 1);
        true  -> Res
    end.

present(Keys, Res) ->
    lists:all(fun (Key) ->
                      X = pget(Key, Res),
                      X =/= [] andalso X =/= undefined
              end, Keys).

assert_single_node(Exp, Act) ->
    ?assertEqual(1, length(Act)),
    assert_node(Exp, hd(Act)).

assert_nodes(Exp, Act0) ->
    Act = [read_node(A) || A <- Act0],
    ?debugVal({Exp, Act}),
    ?assertEqual(length(Exp), length(Act)),
    [?assert(lists:member(E, Act)) || E <- Exp].

assert_node(Exp, Act) ->
    ?assertEqual(Exp, read_node(Act)).

read_node(N) ->
    list_to_atom(hd(string:tokens(binary_to_list(N), "@"))).
