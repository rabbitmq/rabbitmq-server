%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_clustering_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("include/rabbit_mgmt_test.hrl").

-import(rabbit_ct_broker_helpers, [get_node_config/3, restart_node/2]).
-import(rabbit_mgmt_test_util, [http_get/2, http_put/4, http_delete/3]).
-import(rabbit_misc, [pget/2]).

-compile(export_all).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               list_cluster_nodes_test,
                               multi_node_case1_test
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE},
                                                    {rmq_nodes_count, 2}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

list_cluster_nodes_test(Config) ->
    %% see rmq_nodes_count in init_per_suite
    ?assertEqual(2, length(http_get(Config, "/nodes"))),
    passed.

multi_node_case1_test(Config) ->
    Nodename1 = get_node_config(Config, 0, nodename),
    Nodename2 = get_node_config(Config, 1, nodename),
    Policy = [{pattern,    <<".*">>},
              {definition, [{'ha-mode', <<"all">>}]}],
    http_put(Config, "/policies/%2f/HA", Policy, ?NO_CONTENT),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/ha-queue", QArgs, ?NO_CONTENT),

    Q = wait_for(Config, "/queues/%2f/ha-queue"),
    assert_node(Nodename2, pget(node, Q)),
    assert_single_node(Nodename1, pget(slave_nodes, Q)),
    assert_single_node(Nodename1, pget(synchronised_slave_nodes, Q)),
    %% restart node2
    restart_node(Config, 1),

    Q2 = wait_for(Config, "/queues/%2f/ha-queue"),
    assert_node(Nodename1, pget(node, Q2)),
    assert_single_node(Nodename2, pget(slave_nodes, Q2)),
    assert_single_node(Nodename2, pget(synchronised_slave_nodes, Q2)),
    http_delete(Config, "/queues/%2f/ha-queue", ?NO_CONTENT),
    http_delete(Config, "/policies/%2f/HA", ?NO_CONTENT),

    passed.

%%----------------------------------------------------------------------------

wait_for(Config, Path) ->
    wait_for(Config, Path, [slave_nodes, synchronised_slave_nodes]).

wait_for(Config, Path, Keys) ->
    wait_for(Config, Path, Keys, 1000).

wait_for(_Config, Path, Keys, 0) ->
    exit({timeout, {Path, Keys}});

wait_for(Config, Path, Keys, Count) ->
    Res = http_get(Config, Path),
    case present(Keys, Res) of
        false -> timer:sleep(10),
                 wait_for(Config, Path, Keys, Count - 1);
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
    Act = [extract_node(A) || A <- Act0],
    ?assertEqual(length(Exp), length(Act)),
    [?assert(lists:member(E, Act)) || E <- Exp].

assert_node(Exp, Act) ->
    ?assertEqual(Exp, list_to_atom(binary_to_list(Act))).

extract_node(N) ->
    list_to_atom(hd(string:tokens(binary_to_list(N), "@"))).
