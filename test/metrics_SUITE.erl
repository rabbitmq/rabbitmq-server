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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(metrics_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                connection,
                                channel,
                                channel_connection_close,
                                channel_queue_exchange_consumer_close_connection,
                                channel_queue_delete_queue,
                                properties
                               ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(Config,
                                    {rabbit, [
                                              {collect_statistics, fine},
                                              {collect_statistics_interval, 500}
                                             ]}).
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      [ fun merge_app_env/1 ] ++
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

read_table_rpc(Config, Table) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, read_table, [Table]).

read_table(Table) ->
    ets:tab2list(Table).

% node_stats tests are in the management_agent repo

properties(Config) ->
    rabbit_proper_helpers:run_proper(fun prop_connection_metric_count/1, [Config], 25),
    rabbit_proper_helpers:run_proper(fun prop_channel_metric_count/1, [Config], 25),
    rabbit_proper_helpers:run_proper(fun prop_queue_metric_count/1, [Config], 5),
    rabbit_proper_helpers:run_proper(fun prop_queue_metric_count_channel_per_queue/1,
                                     [Config], 5).

prop_connection_metric_count(Config) ->
    ?FORALL(N, {integer(1, 25), resize(100, list(oneof([add, remove])))},
            connection_metric_count(Config, N)).

prop_channel_metric_count(Config) ->
    ?FORALL(N, {integer(1, 25), resize(100, list(oneof([add, remove])))},
            channel_metric_count(Config, N)).

prop_queue_metric_count(Config) ->
    ?FORALL(N, {integer(1, 10), resize(10, list(oneof([add, remove])))},
            queue_metric_count(Config, N)).

prop_queue_metric_count_channel_per_queue(Config) ->
    ?FORALL(N, {integer(1, 10), resize(10, list(oneof([add, remove])))},
            queue_metric_count_channel_per_queue(Config, N)).

connection_metric_count(Config, X) ->
    add_rem_counter(Config, X,
                    {fun rabbit_ct_client_helpers:open_unmanaged_connection/1,
                     fun rabbit_ct_client_helpers:close_connection/1},
                   [ connection_created,
                     connection_metrics,
                     connection_coarse_metrics ]).

channel_metric_count(Config, X) ->
    Conn =  rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    Result = add_rem_counter(Config, X,
                    {fun (_Config) ->
                             {ok, Chan} = amqp_connection:open_channel(Conn),
                             Chan
                     end,
                     fun amqp_channel:close/1},
                   [ channel_created,
                     channel_metrics,
                     channel_process_metrics ]),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    Result.

queue_metric_count(Config, X) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    AddFun = fun (_) ->
                     Queue = declare_queue(Chan),
                     ensure_exchange_metrics_populated(Chan, Queue),
                     ensure_channel_queue_metrics_populated(Chan, Queue),
                     Queue
             end,
    Result = add_rem_counter(Config, X,
                    {AddFun,
                     fun (Q) -> delete_queue(Chan, Q) end},
                   [ channel_queue_metrics,
                     channel_queue_exchange_metrics ]),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    Result.

queue_metric_count_channel_per_queue(Config, X) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    AddFun = fun (_) ->
                     {ok, Chan} = amqp_connection:open_channel(Conn),
                     Queue = declare_queue(Chan),
                     ensure_exchange_metrics_populated(Chan, Queue),
                     ensure_channel_queue_metrics_populated(Chan, Queue),
                     {Chan, Queue}
             end,
    Result = add_rem_counter(Config, X,
                    {AddFun,
                     fun ({Chan, Q}) -> delete_queue(Chan, Q) end},
                   [ channel_queue_metrics,
                     channel_queue_exchange_metrics ]),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    Result.

add_rem_counter(Config, {Initial, Ops}, {AddFun, RemFun}, Tables) ->
    Things = [ AddFun(Config) || _ <- lists:seq(1, Initial) ],
    % either add or remove some things
    {FinalLen, Things1} =
        lists:foldl(fun(add, {L, Items}) ->
                                {L+1, [AddFun(Config) | Items]};
                       (remove, {L, [H|Tail]}) ->
                                RemFun(H),
                                {L-1, Tail};
                       (_, S) -> S end,
                    {Initial, Things},
                    Ops),
    TabLens = lists:map(fun(T) ->
                                length(read_table_rpc(Config, T)) end, Tables),
    [RemFun(Thing) || Thing <- Things1],
    [FinalLen] == lists:usort(TabLens).


connection(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    [_] = read_table_rpc(Config, connection_created),
    [_] = read_table_rpc(Config, connection_metrics),
    [_] = read_table_rpc(Config, connection_coarse_metrics),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    [] = read_table_rpc(Config, connection_created),
    [] = read_table_rpc(Config, connection_metrics),
    [] = read_table_rpc(Config, connection_coarse_metrics).

channel(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    [_] = read_table_rpc(Config, channel_created),
    [_] = read_table_rpc(Config, channel_metrics),
    [_] = read_table_rpc(Config, channel_process_metrics),
    ok = amqp_channel:close(Chan),
    [] = read_table_rpc(Config, channel_created),
    [] = read_table_rpc(Config, channel_metrics),
    [] = read_table_rpc(Config, channel_process_metrics),
    ok = rabbit_ct_client_helpers:close_connection(Conn).

channel_connection_close(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, _} = amqp_connection:open_channel(Conn),
    [_] = read_table_rpc(Config, channel_created),
    [_] = read_table_rpc(Config, channel_metrics),
    [_] = read_table_rpc(Config, channel_process_metrics),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    [] = read_table_rpc(Config, channel_created),
    [] = read_table_rpc(Config, channel_metrics),
    [] = read_table_rpc(Config, channel_process_metrics).

channel_queue_delete_queue(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queue = declare_queue(Chan),
    ensure_exchange_metrics_populated(Chan, Queue),
    ensure_channel_queue_metrics_populated(Chan, Queue),
    [_] = read_table_rpc(Config, channel_queue_metrics),
    [_] = read_table_rpc(Config, channel_queue_exchange_metrics),

    delete_queue(Chan, Queue),
    % ensure  removal of queue cleans up channel_queue metrics
    [] = read_table_rpc(Config, channel_queue_exchange_metrics),
    [] = read_table_rpc(Config, channel_queue_metrics),
    ok = rabbit_ct_client_helpers:close_connection(Conn).

channel_queue_exchange_consumer_close_connection(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queue = declare_queue(Chan),
    ensure_exchange_metrics_populated(Chan, Queue),

    [_] = read_table_rpc(Config, channel_exchange_metrics),
    [_] = read_table_rpc(Config, channel_queue_exchange_metrics),

    ensure_channel_queue_metrics_populated(Chan, Queue),
    [_] = read_table_rpc(Config, channel_queue_metrics),

    Sub = #'basic.consume'{queue = Queue},
      #'basic.consume_ok'{consumer_tag = _} =
        amqp_channel:call(Chan, Sub),

    [_] = read_table_rpc(Config, consumer_created),

    ok = rabbit_ct_client_helpers:close_connection(Conn),
    % ensure cleanup happened
    [] = read_table_rpc(Config, channel_exchange_metrics),
    [] = read_table_rpc(Config, channel_queue_exchange_metrics),
    [] = read_table_rpc(Config, channel_queue_metrics),
    [] = read_table_rpc(Config, consumer_created).


declare_queue(Chan) ->
    Declare = #'queue.declare'{},
    #'queue.declare_ok'{queue = Name} = amqp_channel:call(Chan, Declare),
    Name.

delete_queue(Chan, Name) ->
    Delete = #'queue.delete'{queue = Name},
    #'queue.delete_ok'{} = amqp_channel:call(Chan, Delete).

ensure_exchange_metrics_populated(Chan, RoutingKey) ->
    % need to publish for exchange metrics to be populated
    Publish = #'basic.publish'{routing_key = RoutingKey},
    amqp_channel:call(Chan, Publish, #amqp_msg{payload = <<"hello">>}),
    timer:sleep(750). % TODO: send emit_stats message to channel instead of sleeping?

ensure_channel_queue_metrics_populated(Chan, Queue) ->
    % need to get and wait for timer for channel queue metrics to be populated
    Get = #'basic.get'{queue = Queue, no_ack=true},
    {#'basic.get_ok'{}, #amqp_msg{}} = amqp_channel:call(Chan, Get),
    timer:sleep(750).
