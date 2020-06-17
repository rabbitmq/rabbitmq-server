%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(list_consumers_sanity_check_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, list_consumers_sanity_check}
    ].

groups() ->
    [
      {list_consumers_sanity_check, [], [
          list_consumers_sanity_check
        ]}
    ].

group(_) ->
    [].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
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
%% Testcase
%% -------------------------------------------------------------------

list_consumers_sanity_check(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Chan = rabbit_ct_client_helpers:open_channel(Config, A),
    %% this queue is not cleaned up because the entire node is
    %% reset between tests
    QName = <<"list_consumers_q">>,
    #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{queue = QName}),

    %% No consumers even if we have some queues
    [] = rabbitmqctl_list_consumers(Config, A),

    %% Several consumers on single channel should be correctly reported
    #'basic.consume_ok'{consumer_tag = CTag1} = amqp_channel:call(Chan, #'basic.consume'{queue = QName}),
    #'basic.consume_ok'{consumer_tag = CTag2} = amqp_channel:call(Chan, #'basic.consume'{queue = QName}),
    true = (lists:sort([CTag1, CTag2]) =:=
            lists:sort(rabbitmqctl_list_consumers(Config, A))),

    %% `rabbitmqctl report` shares some code with `list_consumers`, so
    %% check that it also reports both channels
    {ok, ReportStdOut} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A,
      ["list_consumers", "--no-table-headers"]),
    ReportLines = re:split(ReportStdOut, <<"\n">>, [trim]),
    ReportCTags = [lists:nth(3, re:split(Row, <<"\t">>)) || <<"list_consumers_q", _/binary>> = Row <- ReportLines],
    true = (lists:sort([CTag1, CTag2]) =:=
            lists:sort(ReportCTags)).

rabbitmqctl_list_consumers(Config, Node) ->
    {ok, StdOut} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Node,
      ["list_consumers", "--no-table-headers"]),
    [<<"Listing consumers", _/binary>> | ConsumerRows] = re:split(StdOut, <<"\n">>, [trim]),
    CTags = [ lists:nth(3, re:split(Row, <<"\t">>)) || Row <- ConsumerRows ],
    CTags.

list_queues_online_and_offline(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    %% Node B will be stopped
    BCh = rabbit_ct_client_helpers:open_channel(Config, B),
    #'queue.declare_ok'{} = amqp_channel:call(ACh, #'queue.declare'{queue = <<"q_a_1">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(ACh, #'queue.declare'{queue = <<"q_a_2">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(BCh, #'queue.declare'{queue = <<"q_b_1">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(BCh, #'queue.declare'{queue = <<"q_b_2">>, durable = true}),

    rabbit_ct_broker_helpers:rabbitmqctl(Config, B, ["stop"]),

    GotUp = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "--online", "name", "--no-table-headers"])),
    ExpectUp = [[<<"q_a_1">>], [<<"q_a_2">>]],
    ExpectUp = GotUp,

    GotDown = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "--offline", "name", "--no-table-headers"])),
    ExpectDown = [[<<"q_b_1">>], [<<"q_b_2">>]],
    ExpectDown = GotDown,

    GotAll = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "name", "--no-table-headers"])),
    ExpectAll = ExpectUp ++ ExpectDown,
    ExpectAll = GotAll,

    ok.
