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
%% Copyright (c) 2016-2019 Pivotal Software, Inc.  All rights reserved.
%%
-module(metrics_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").


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
                                connection_metric_count_test,
                                channel_metric_count_test,
                                queue_metric_count_test,
                                queue_metric_count_channel_per_queue_test,
                                connection_metric_idemp_test,
                                channel_metric_idemp_test,
                                queue_metric_idemp_test
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
    clean_core_metrics(Config),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

% NB: node_stats tests are in the management_agent repo

connection_metric_count_test(Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_connection_metric_count/1, [Config], 25).

channel_metric_count_test(Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_channel_metric_count/1, [Config], 25).

queue_metric_count_test(Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_queue_metric_count/1, [Config], 5).

queue_metric_count_channel_per_queue_test(Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_queue_metric_count_channel_per_queue/1,
                                     [Config], 5).

connection_metric_idemp_test(Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_connection_metric_idemp/1, [Config], 25).

channel_metric_idemp_test(Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_channel_metric_idemp/1, [Config], 25).

queue_metric_idemp_test(Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_queue_metric_idemp/1, [Config], 25).

prop_connection_metric_idemp(Config) ->
    ?FORALL(N, {integer(1, 25), integer(1, 25)},
            connection_metric_idemp(Config, N)).

prop_channel_metric_idemp(Config) ->
    ?FORALL(N, {integer(1, 25), integer(1, 25)},
            channel_metric_idemp(Config, N)).

prop_queue_metric_idemp(Config) ->
    ?FORALL(N, {integer(1, 25), integer(1, 25)},
            queue_metric_idemp(Config, N)).

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

connection_metric_idemp(Config, {N, R}) ->
    Conns = [rabbit_ct_client_helpers:open_unmanaged_connection(Config)
               || _ <- lists:seq(1, N)],
    Table = [ Pid || {Pid, _} <- read_table_rpc(Config, connection_metrics)],
    Table2 = [ Pid || {Pid, _} <- read_table_rpc(Config, connection_coarse_metrics)],
    % refresh stats 'R' times
    [[Pid ! emit_stats || Pid <- Table] || _ <- lists:seq(1, R)],
    force_metric_gc(Config),
    TableAfter = [ Pid || {Pid, _} <- read_table_rpc(Config, connection_metrics)],
    TableAfter2 = [ Pid || {Pid, _} <- read_table_rpc(Config, connection_coarse_metrics)],
    [rabbit_ct_client_helpers:close_connection(Conn) || Conn <- Conns],
    (Table2 == TableAfter2) and (Table == TableAfter) and
    (N == length(Table)) and (N == length(TableAfter)).

channel_metric_idemp(Config, {N, R}) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    [amqp_connection:open_channel(Conn) || _ <- lists:seq(1, N)],
    Table = [ Pid || {Pid, _} <- read_table_rpc(Config, channel_metrics)],
    Table2 = [ Pid || {Pid, _} <- read_table_rpc(Config, channel_process_metrics)],
    % refresh stats 'R' times
    [[Pid ! emit_stats || Pid <- Table] || _ <- lists:seq(1, R)],
    force_metric_gc(Config),
    TableAfter = [ Pid || {Pid, _} <- read_table_rpc(Config, channel_metrics)],
    TableAfter2 = [ Pid || {Pid, _} <- read_table_rpc(Config, channel_process_metrics)],
    rabbit_ct_client_helpers:close_connection(Conn),
    (Table2 == TableAfter2) and (Table == TableAfter) and
    (N == length(Table)) and (N == length(TableAfter)).

queue_metric_idemp(Config, {N, R}) ->
    clean_core_metrics(Config),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queues =
        [begin
              Queue = declare_queue(Chan),
              ensure_exchange_metrics_populated(Chan, Queue),
              ensure_channel_queue_metrics_populated(Chan, Queue),
              Queue
          end || _ <- lists:seq(1, N)],

    Table = [ Pid || {Pid, _, _} <- read_table_rpc(Config, queue_metrics)],
    Table2 = [ Pid || {Pid, _, _} <- read_table_rpc(Config, queue_coarse_metrics)],
    % refresh stats 'R' times
    ChanTable = read_table_rpc(Config, channel_created),
    [[Pid ! emit_stats || {Pid, _, _} <- ChanTable ] || _ <- lists:seq(1, R)],
    force_metric_gc(Config),
    TableAfter = [ Pid || {Pid, _, _} <- read_table_rpc(Config,  queue_metrics)],
    TableAfter2 = [ Pid || {Pid, _, _} <- read_table_rpc(Config, queue_coarse_metrics)],
    [ delete_queue(Chan, Q) || Q <- Queues],
    rabbit_ct_client_helpers:close_connection(Conn),
    (Table2 == TableAfter2) and (Table == TableAfter) and
    (N == length(Table)) and (N == length(TableAfter)).

connection_metric_count(Config, Ops) ->
    add_rem_counter(Config, Ops,
                    {fun rabbit_ct_client_helpers:open_unmanaged_connection/1,
                     fun(Cfg) ->
                             rabbit_ct_client_helpers:close_connection(Cfg)
                     end},
                   [ connection_created,
                     connection_metrics,
                     connection_coarse_metrics ]).

channel_metric_count(Config, Ops) ->
    Conn =  rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    Result = add_rem_counter(Config, Ops,
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

queue_metric_count(Config, Ops) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    AddFun = fun (_) ->
                     Queue = declare_queue(Chan),
                     ensure_exchange_metrics_populated(Chan, Queue),
                     ensure_channel_queue_metrics_populated(Chan, Queue),
                     force_channel_stats(Config),
                     Queue
             end,
    Result = add_rem_counter(Config, Ops,
                    {AddFun,
                     fun (Q) -> delete_queue(Chan, Q),
                                force_metric_gc(Config)
                     end}, [channel_queue_metrics,
                            channel_queue_exchange_metrics ]),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    Result.

queue_metric_count_channel_per_queue(Config, Ops) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    AddFun = fun (_) ->
                     {ok, Chan} = amqp_connection:open_channel(Conn),
                     Queue = declare_queue(Chan),
                     ensure_exchange_metrics_populated(Chan, Queue),
                     ensure_channel_queue_metrics_populated(Chan, Queue),
                     force_channel_stats(Config),
                     {Chan, Queue}
             end,
    Result = add_rem_counter(Config, Ops,
                    {AddFun,
                     fun ({Chan, Q}) ->
                             delete_queue(Chan, Q),
                             force_metric_gc(Config)
                     end},
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
    force_metric_gc(Config),
    TabLens = lists:map(fun(T) ->
                                length(read_table_rpc(Config, T))
                        end, Tables),
    [RemFun(Thing) || Thing <- Things1],
    [FinalLen] == lists:usort(TabLens).


connection(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    [_] = read_table_rpc(Config, connection_created),
    [_] = read_table_rpc(Config, connection_metrics),
    [_] = read_table_rpc(Config, connection_coarse_metrics),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    force_metric_gc(Config),
    [] = read_table_rpc(Config, connection_created),
    [] = read_table_rpc(Config, connection_metrics),
    [] = read_table_rpc(Config, connection_coarse_metrics),
    ok.

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
    force_channel_stats(Config),
    [_] = read_table_rpc(Config, channel_queue_metrics),
    [_] = read_table_rpc(Config, channel_queue_exchange_metrics),

    delete_queue(Chan, Queue),
    force_metric_gc(Config),
    % ensure  removal of queue cleans up channel_queue metrics
    [] = read_table_rpc(Config, channel_queue_exchange_metrics),
    [] = read_table_rpc(Config, channel_queue_metrics),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    ok.

channel_queue_exchange_consumer_close_connection(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queue = declare_queue(Chan),
    ensure_exchange_metrics_populated(Chan, Queue),
    force_channel_stats(Config),

    [_] = read_table_rpc(Config, channel_exchange_metrics),
    [_] = read_table_rpc(Config, channel_queue_exchange_metrics),

    ensure_channel_queue_metrics_populated(Chan, Queue),
    force_channel_stats(Config),
    [_] = read_table_rpc(Config, channel_queue_metrics),

    Sub = #'basic.consume'{queue = Queue},
      #'basic.consume_ok'{consumer_tag = _} =
        amqp_channel:call(Chan, Sub),

    [_] = read_table_rpc(Config, consumer_created),

    ok = rabbit_ct_client_helpers:close_connection(Conn),
    % ensure cleanup happened
    force_metric_gc(Config),
    [] = read_table_rpc(Config, channel_exchange_metrics),
    [] = read_table_rpc(Config, channel_queue_exchange_metrics),
    [] = read_table_rpc(Config, channel_queue_metrics),
    [] = read_table_rpc(Config, consumer_created),
    ok.



%% -------------------------------------------------------------------
%% Utilities
%% -------------------------------------------------------------------

declare_queue(Chan) ->
    Declare = #'queue.declare'{durable = false, auto_delete = true},
    #'queue.declare_ok'{queue = Name} = amqp_channel:call(Chan, Declare),
    Name.

delete_queue(Chan, Name) ->
    Delete = #'queue.delete'{queue = Name},
    #'queue.delete_ok'{} = amqp_channel:call(Chan, Delete).

ensure_exchange_metrics_populated(Chan, RoutingKey) ->
    % need to publish for exchange metrics to be populated
    Publish = #'basic.publish'{routing_key = RoutingKey},
    amqp_channel:call(Chan, Publish, #amqp_msg{payload = <<"hello">>}).

ensure_channel_queue_metrics_populated(Chan, Queue) ->
    % need to get and wait for timer for channel queue metrics to be populated
    Get = #'basic.get'{queue = Queue, no_ack=true},
    {#'basic.get_ok'{}, #amqp_msg{}} = amqp_channel:call(Chan, Get).

force_channel_stats(Config) ->
    [ Pid ! emit_stats || {Pid, _} <- read_table_rpc(Config, channel_created) ],
    timer:sleep(100).

read_table_rpc(Config, Table) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, read_table, [Table]).

clean_core_metrics(Config) ->
    [ rabbit_ct_broker_helpers:rpc(Config, 0, ets, delete_all_objects, [Table])
      || {Table, _} <- ?CORE_TABLES].

read_table(Table) ->
    ets:tab2list(Table).

force_metric_gc(Config) ->
    timer:sleep(300),
    rabbit_ct_broker_helpers:rpc(Config, 0, erlang, send,
                                 [rabbit_core_metrics_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, 0, gen_server, call,
                                 [rabbit_core_metrics_gc, test]).
