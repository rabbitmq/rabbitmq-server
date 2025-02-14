-module(queue_type_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(TIMEOUT, 30_000).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, classic},
     {group, quorum},
     {group, stream}
    ].


all_tests() ->
    [
     smoke,
     ack_after_queue_delete
    ].

groups() ->
    [
     {classic, [], all_tests()},
     {quorum, [], all_tests()},
     {stream, [],
      [
       stream
      ]}
    ].

init_per_suite(Config0) ->
    Tick = 256,
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, [
                                  {quorum_tick_interval, Tick},
                                  {stream_tick_interval, Tick}
                                 ]}),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config),
    ok.

init_per_group(Group, Config) ->
    ClusterSize = 3,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base, {skip_n_nodes, ClusterSize}}
                                            ]),
    Config1b = rabbit_ct_helpers:set_config(Config1,
                                            [{queue_type, atom_to_binary(Group, utf8)}
                                            ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1b,
                                          [fun merge_app_env/1 ] ++
                                          rabbit_ct_broker_helpers:setup_steps()),
    case Config2 of
        {skip, _Reason} = Skip ->
            %% To support mixed-version clusters,
            %% Khepri feature flag is unsupported
            Skip;
        _ ->
            ok = rabbit_ct_broker_helpers:rpc(
                   Config2, 0, application, set_env,
                   [rabbit, channel_tick_interval, 100]),
            Config2
    end.

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit,
                                       [{core_metrics_gc_interval, 100},
                                       {log, [{file, [{level, debug}]}]}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2,
                                rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    catch delete_queues(),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%%%===================================================================
%%% Test cases
%%%===================================================================

smoke(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish_and_confirm(Ch, QName, <<"msg1">>),
    DTag = basic_get(Ch, QName),

    basic_ack(Ch, DTag),
    basic_get_empty(Ch, QName),

    %% consume
    publish_and_confirm(Ch, QName, <<"msg2">>),
    ConsumerTag1 = <<"ctag1">>,
    ok = subscribe(Ch, QName, ConsumerTag1),
    %% receive and ack
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{}} ->
            basic_ack(Ch, DeliveryTag)
    after ?TIMEOUT ->
              flush(),
              exit(basic_deliver_timeout)
    end,
    basic_cancel(Ch, ConsumerTag1),

    %% assert empty
    basic_get_empty(Ch, QName),

    %% consume and nack
    ConsumerTag2 = <<"ctag2">>,
    ok = subscribe(Ch, QName, ConsumerTag2),
    publish_and_confirm(Ch, QName, <<"msg3">>),
    receive
        {#'basic.deliver'{delivery_tag = T,
                          redelivered  = false},
         #amqp_msg{}} ->
            basic_cancel(Ch, ConsumerTag2),
            basic_nack(Ch, T)
    after ?TIMEOUT ->
              exit(basic_deliver_timeout)
    end,
    %% get and ack
    basic_ack(Ch, basic_get(Ch, QName)),
    %% global counters
    ok = publish_and_confirm(Ch, <<"non-existent_queue">>, <<"msg4">>),
    ConsumerTag3 = <<"ctag3">>,
    ok = subscribe(Ch, QName, ConsumerTag3),
    ProtocolCounters = maps:get([{protocol, amqp091}], get_global_counters(Config)),
    ?assertEqual(#{
                   messages_confirmed_total => 4,
                   messages_received_confirm_total => 4,
                   messages_received_total => 4,
                   messages_routed_total => 3,
                   messages_unroutable_dropped_total => 1,
                   messages_unroutable_returned_total => 0,
                   consumers => 1,
                   publishers => 1
                  }, ProtocolCounters),
    QueueType = list_to_atom(
                  "rabbit_" ++
                  binary_to_list(?config(queue_type, Config)) ++
                  "_queue"),
    ProtocolQueueTypeCounters = maps:get([{protocol, amqp091}, {queue_type, QueueType}],
                                         get_global_counters(Config)),
    ?assertEqual(#{
                   messages_acknowledged_total => 3,
                   messages_delivered_consume_auto_ack_total => 0,
                   messages_delivered_consume_manual_ack_total => 0,
                   messages_delivered_get_auto_ack_total => 0,
                   messages_delivered_get_manual_ack_total => 0,
                   messages_delivered_total => 4,
                   messages_get_empty_total => 2,
                   messages_redelivered_total => 1
                  }, ProtocolQueueTypeCounters),


    ok = rabbit_ct_client_helpers:close_channel(Ch),

    ?assertMatch(
       #{consumers := 0,
         publishers := 0},
       maps:get([{protocol, amqp091}], get_global_counters(Config))),

    ok.

ack_after_queue_delete(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish_and_confirm(Ch, QName, <<"msg1">>),
    DTag = basic_get(Ch, QName),

    ChRef = erlang:monitor(process, Ch),
    #'queue.delete_ok'{} = delete(Ch, QName),

    basic_ack(Ch, DTag),
    %% assert no channel error
    receive
        {'DOWN', ChRef, process, _, _} ->
            ct:fail("unexpected channel closure")
    after 1000 ->
              ok
    end,
    flush(),
    ok.

stream(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish_and_confirm(Ch, QName, <<"msg1">>),
    Args = [{<<"x-stream-offset">>, longstr, <<"last">>}],

    SubCh = rabbit_ct_client_helpers:open_channel(Config, 2),
    qos(SubCh, 10, false),
    ok = queue_utils:wait_for_local_stream_member(2, <<"/">>, QName, Config),

    try
        amqp_channel:subscribe(
          SubCh, #'basic.consume'{queue = QName,
                                  consumer_tag = <<"ctag">>,
                                  arguments = Args},
          self()),
        receive
            {#'basic.deliver'{delivery_tag = T,
                              redelivered  = false},
             #amqp_msg{}} ->
                basic_ack(SubCh, T)
        after ?TIMEOUT ->
                  exit(basic_deliver_timeout)
        end
    catch
        _:Err ->
            ct:pal("basic.consume error ~p", [Err]),
            exit(Err)
    end,


    ok.

%% Utility

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue = Q,
                                           durable = true,
                                           auto_delete = false,
                                           arguments = Args}).

delete(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).

publish_and_confirm(Ch, Queue, Msg) ->
    publish(Ch, Queue, Msg),
    ct:pal("xwaiting for ~ts message confirmation from ~ts", [Msg, Queue]),
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after ?TIMEOUT ->
                   flush(),
                   exit(confirm_timeout)
         end.

basic_get(Ch, Queue) ->
    {GetOk, _} = Reply = amqp_channel:call(Ch, #'basic.get'{queue = Queue,
                                                            no_ack = false}),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{}}, Reply),
    GetOk#'basic.get_ok'.delivery_tag.

basic_get_empty(Ch, Queue) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Queue,
                                                    no_ack = false})).

subscribe(Ch, Queue, CTag) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = false,
                                                consumer_tag = CTag},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
             ok
    after ?TIMEOUT ->
              exit(basic_consume_timeout)
    end.

basic_ack(Ch, DTag) ->
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag,
                                       multiple = false}).

basic_cancel(Ch, CTag) ->
    #'basic.cancel_ok'{} =
        amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}).

basic_nack(Ch, DTag) ->
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DTag,
                                        requeue = true,
                                        multiple = false}).

flush() ->
    receive
        Any ->
            ct:pal("flush ~tp", [Any]),
            flush()
    after 0 ->
              ok
    end.

get_global_counters(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_global_counters, overview, []).

qos(Ch, Prefetch, Global) ->
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = Global,
                                                    prefetch_count = Prefetch})).
