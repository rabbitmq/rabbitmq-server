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
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(policy_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, cluster_size_2}
    ].

groups() ->
    [
     {cluster_size_2, [], [
                           policy_ttl,
                           operator_policy_ttl,
                           operator_retroactive_policy_ttl,
                           operator_retroactive_policy_publish_ttl
                          ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_2, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, 2},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:setup_steps(),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:teardown_steps(),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

policy_ttl(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_ttl-queue">>,
    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"ttl-policy">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 20}]),

    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 20)),
    timer:sleep(50),
    get_empty(Ch, Q),
    delete(Ch, Q),

    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl-policy">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

operator_policy_ttl(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_ttl-queue">>,
    % Operator policy will override
    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"ttl-policy">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 100000}]),
    rabbit_ct_broker_helpers:set_operator_policy(Config, 0, <<"ttl-policy-op">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 1}]),

    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 50)),
    timer:sleep(50),
    get_empty(Ch, Q),
    delete(Ch, Q),

    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl-policy">>),
    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"ttl-policy-op">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

operator_retroactive_policy_ttl(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_ttl-queue">>,
    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 50)),
    % Operator policy will override
    rabbit_ct_broker_helpers:set_operator_policy(Config, 0, <<"ttl-policy-op">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 1}]),

    %% Old messages are not expired
    timer:sleep(50),
    get_messages(50, Ch, Q),
    delete(Ch, Q),

    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"ttl-policy-op">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

operator_retroactive_policy_publish_ttl(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_ttl-queue">>,
    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 50)),
    % Operator policy will override
    rabbit_ct_broker_helpers:set_operator_policy(Config, 0, <<"ttl-policy-op">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 1}]),

    %% Old messages are not expired, new ones only expire when they get to the head of
    %% the queue
    publish(Ch, Q, lists:seq(1, 25)),
    timer:sleep(50),
    [[<<"policy_ttl-queue">>, <<"75">>]] =
        rabbit_ct_broker_helpers:rabbitmqctl_list(Config, 0, ["list_queues", "--no-table-headers"]),
    get_messages(50, Ch, Q),
    delete(Ch, Q),

    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"ttl-policy-op">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

%%----------------------------------------------------------------------------


declare(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true}).

delete(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish(Ch, Q, Ps) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [publish1(Ch, Q, P) || P <- Ps],
    amqp_channel:wait_for_confirms(Ch).

publish1(Ch, Q, P) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = erlang:md5(term_to_binary(P))}).

publish1(Ch, Q, P, Pd) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = Pd}).

props(undefined) -> #'P_basic'{delivery_mode = 2};
props(P)         -> #'P_basic'{priority      = P,
                               delivery_mode = 2}.

consume(Ch, Q, Ack) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue        = Q,
                                                no_ack       = Ack =:= no_ack,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end.

get_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = Q}).

get_messages(0, Ch, Q) ->
    get_empty(Ch, Q);
get_messages(Number, Ch, Q) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = Q}) of
        {#'basic.get_ok'{}, _} ->
            get_messages(Number - 1, Ch, Q);
        #'basic.get_empty'{} ->
            exit(failed)
    end.

%%----------------------------------------------------------------------------
