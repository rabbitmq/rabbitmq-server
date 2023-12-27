-module(message_containers_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(FEATURE_FLAG, message_containers).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, classic},
     {group, quorum},
     {group, stream}
    ].


groups() ->
    [
     {classic, [], all_tests()},
     {quorum, [], all_tests()},
     {stream, [], all_tests()}
    ].

all_tests() ->
    [
     enable_ff
    ].

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, [{quorum_tick_interval, 1000}]}),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config),
    ok.

init_per_group(Group, Config) ->
    ct:pal("init per group ~p", [Group]),
    ClusterSize = 3,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    Config1b = rabbit_ct_helpers:set_config(Config1,
                                            [{queue_type, atom_to_binary(Group, utf8)},
                                             {net_ticktime, 10}]),

    Config1c = rabbit_ct_helpers:merge_app_env(
                 Config1b, {rabbit, [{forced_feature_flags_on_init, []}]}),
    Config2 = rabbit_ct_helpers:run_steps(Config1c,
                                          [fun merge_app_env/1 ] ++
                                          rabbit_ct_broker_helpers:setup_steps()),
    case Config2 of
        {skip, _} ->
            Config2;
        _ ->
            ok = rabbit_ct_broker_helpers:rpc(
                   Config2, 0, application, set_env,
                   [rabbit, channel_tick_interval, 100]),

            AllFFs = rabbit_ct_broker_helpers:rpc(Config2, rabbit_feature_flags, list, [all, stable]),
            FFs = maps:keys(maps:remove(?FEATURE_FLAG, AllFFs)),
            ct:pal("FFs ~p", [FFs]),
            case Group of
                classic ->
                    try
                        rabbit_ct_broker_helpers:set_policy(
                          Config2, 0,
                          <<"ha-policy">>, <<".*">>, <<"queues">>,
                          [{<<"ha-mode">>, <<"all">>}]),
                        Config2
                    catch
                        _:{badmatch, {error_string, Reason}} ->
                            rabbit_ct_helpers:run_steps(
                              Config2,
                              rabbit_ct_broker_helpers:teardown_steps()),
                            {skip, Reason}
                    end;
                _ ->
                    Config2
            end
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
    case rabbit_ct_broker_helpers:is_feature_flag_supported(Config, ?FEATURE_FLAG) of
        false ->
            {skip, "feature flag message_containers is unsupported"};
        true ->
            Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
            ?assertNot(rabbit_ct_broker_helpers:is_feature_flag_enabled(Config, ?FEATURE_FLAG)),
            Q = rabbit_data_coercion:to_binary(Testcase),
            Config2 = rabbit_ct_helpers:set_config(Config1,
                                                   [{queue_name, Q},
                                                    {alt_queue_name, <<Q/binary, "_alt">>}
                                                   ]),
            rabbit_ct_helpers:run_steps(Config2,
                                        rabbit_ct_client_helpers:setup_steps())
    end.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%%%===================================================================
%%% Test cases
%%%===================================================================

enable_ff(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    timer:sleep(100),

    ConsumerTag1 = <<"ctag1">>,
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, 2),
    qos(Ch2, 2),
    ok = subscribe(Ch2, QName, ConsumerTag1),
    publish_and_confirm(Ch, QName, <<"msg1">>),

    receive_and_ack(Ch2),
    %% consume
    publish(Ch, QName, <<"msg2">>),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FEATURE_FLAG),

    confirm(),
    publish_and_confirm(Ch, QName, <<"msg3">>),
    receive_and_ack(Ch2),
    receive_and_ack(Ch2).

receive_and_ack(Ch) ->
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{}} ->
            basic_ack(Ch, DeliveryTag)
    after 5000 ->
              flush(),
              exit(basic_deliver_timeout)
    end.

%% Utility

delete_queues() ->
    [{ok, 0} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
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
                           #amqp_msg{props = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).

publish_and_confirm(Ch, Queue, Msg) ->
    publish(Ch, Queue, Msg),
    ct:pal("waiting for ~ts message confirmation from ~ts", [Msg, Queue]),
    confirm().

confirm() ->
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 2500 ->
                   flush(),
                   exit(confirm_timeout)
         end.

subscribe(Ch, Queue, CTag) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = false,
                                                consumer_tag = CTag},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
             ok
    after 5000 ->
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

qos(Ch, Prefetch) ->
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{prefetch_count = Prefetch})).
