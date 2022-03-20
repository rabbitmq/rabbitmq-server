%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_gen_server2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, sequential_tests}
    ].

groups() ->
    [
      {sequential_tests, [], [
          gen_server2_with_state,
          mcall
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

gen_server2_with_state(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, gen_server2_with_state1, [Config]).

gen_server2_with_state1(_Config) ->
    fhc_state = gen_server2:with_state(file_handle_cache,
                                       fun (S) -> element(1, S) end),
    passed.


mcall(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, mcall1, [Config]).

mcall1(_Config) ->
    P1 = spawn(fun gs2_test_listener/0),
    register(foo, P1),
    global:register_name(gfoo, P1),

    P2 = spawn(fun() -> exit(bang) end),
    %% ensure P2 is dead (ignore the race setting up the monitor)
    await_exit(P2),

    P3 = spawn(fun gs2_test_crasher/0),

    %% since P2 crashes almost immediately and P3 after receiving its first
    %% message, we have to spawn a few more processes to handle the additional
    %% cases we're interested in here
    register(baz, spawn(fun gs2_test_crasher/0)),
    register(bog, spawn(fun gs2_test_crasher/0)),
    global:register_name(gbaz, spawn(fun gs2_test_crasher/0)),

    NoNode = rabbit_nodes:make("nonode"),

    Targets =
        %% pids
        [P1, P2, P3]
        ++
        %% registered names
        [foo, bar, baz]
        ++
        %% {Name, Node} pairs
        [{foo, node()}, {bar, node()}, {bog, node()}, {foo, NoNode}]
        ++
        %% {global, Name}
        [{global, gfoo}, {global, gbar}, {global, gbaz}],

    GoodResults = [{D, goodbye} || D <- [P1, foo,
                                         {foo, node()},
                                         {global, gfoo}]],

    BadResults  = [{P2,             noproc},   % died before use
                   {P3,             boom},     % died on first use
                   {bar,            noproc},   % never registered
                   {baz,            boom},     % died on first use
                   {{bar, node()},  noproc},   % never registered
                   {{bog, node()},  boom},     % died on first use
                   {{foo, NoNode},  nodedown}, % invalid node
                   {{global, gbar}, noproc},   % never registered globally
                   {{global, gbaz}, boom}],    % died on first use

    {Replies, Errors} = gen_server2:mcall([{T, hello} || T <- Targets]),
    true = lists:sort(Replies) == lists:sort(GoodResults),
    true = lists:sort(Errors)  == lists:sort(BadResults),

    %% cleanup (ignore the race setting up the monitor)
    P1 ! stop,
    await_exit(P1),
    passed.

await_exit(Pid) ->
    MRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MRef, _, _, _} -> ok
    end.

gs2_test_crasher() ->
    receive
        {'$gen_call', _From, hello} -> exit(boom)
    end.

gs2_test_listener() ->
    receive
        {'$gen_call', From, hello} ->
            gen_server2:reply(From, goodbye),
            gs2_test_listener();
        stop ->
            ok
    end.
