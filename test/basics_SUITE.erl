-module(basics_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, single_node}
    ].

groups() ->
    [
     {classic, [], all_tests()},
     {quorum, [], all_tests()},
     {clustered, [], [{cluster_size_3, [], [ ]}]}
    ].

all_tests() ->
    [
     roundtrip
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config,
                                 [{rmq_nodes_clustered, true},
                                  {queue_type, <<"classic">>}]);
init_per_group(Group, Config) ->
    ClusterSize = case Group of
                      cluster_size_2 -> 2;
                      cluster_size_3 -> 3;
                      cluster_size_5 -> 5;
                      _ -> 1
                  end,
    QueueType = atom_to_binary(Group, utf8),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {queue_type, QueueType},
                                            {tcp_ports_base}]),
    Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
    Config2 = rabbit_ct_helpers:run_steps(Config1b,
                                          [fun merge_app_env/1 ] ++
                                          rabbit_ct_broker_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:rpc(
           Config2, 0, application, set_env,
           [rabbit, channel_queue_cleanup_interval, 100]),
    %% HACK: the larger cluster sizes benefit for a bit more time
    %% after clustering before running the tests.
    case Group of
        cluster_size_5 ->
            timer:sleep(5000),
            Config2;
        _ ->
            Config2
    end.

end_per_group(clustered, Config) ->
    Config;
end_per_group(unclustered, Config) ->
    Config;
end_per_group(clustered_with_partitions, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

end_per_testcase(Testcase, Config) when Testcase == reconnect_consumer_and_publish;
                                        Testcase == reconnect_consumer_and_wait;
                                        Testcase == reconnect_consumer_and_wait_channel_down ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase);
end_per_testcase(Testcase, Config) ->
    catch delete_queues(),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
roundtrip(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    QueueType = ?config(queue_type, Config),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName,
                         [{<<"x-queue-type">>, longstr, QueueType}])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    publish_confirm(Ch, QName),
    subscribe(Ch, QName, false),
    receive
        {#'basic.deliver'{delivery_tag = Tag}, _} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Tag}),
            ok
    after 5000 ->
              exit(delivery_timeout)
    end,
    ok.

%% Util
delete_queues(Ch, Queues) ->
    [amqp_channel:call(Ch, #'queue.delete'{queue = Q}) ||  Q <- Queues].

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

publish_confirm(Ch, QName) ->
    publish(Ch, QName),
    amqp_channel:register_confirm_handler(Ch, self()),
    ct:pal("waiting for confirms from ~s", [QName]),
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 2500 ->
                   flush(100),
                   exit(confirm_timeout)
         end,
    ct:pal("CONFIRMED! ~s", [QName]),
    ok.

subscribe(Ch, Queue, NoAck) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end.

publish(Ch, Queue) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props = #'P_basic'{},
                                     payload = <<"msg">>}).
declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue = Q,
                                           durable = true,
                                           auto_delete = false,
                                           arguments = Args}).
flush(T) ->
    receive X ->
                ct:pal("flushed ~w", [X]),
                flush(T)
    after T ->
              ok
    end.
