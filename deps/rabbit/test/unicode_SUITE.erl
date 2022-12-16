-module(unicode_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

%% Unicode U+1F407
-define(UNICODE_STRING, "bunny🐇bunny").

all() ->
    [
     {group, queues}
    ].

groups() ->
    [
     {queues, [], [
                   classic_queue_v1,
                   classic_queue_v2,
                   quorum_queue,
                   stream
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

init_per_group(Group, Config0) ->
    PrivDir0 = ?config(priv_dir, Config0),
    PrivDir = filename:join(PrivDir0, ?UNICODE_STRING),
    ok = file:make_dir(PrivDir),
    Config = rabbit_ct_helpers:set_config(Config0, [{priv_dir, PrivDir},
                                                    {rmq_nodename_suffix, Group}]),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()
                               ).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

classic_queue_v1(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env, [rabbit, classic_queue_default_version, 1]),
    ok = queue(Config, ?FUNCTION_NAME, []).

classic_queue_v2(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env, [rabbit, classic_queue_default_version, 2]),
    ok = queue(Config, ?FUNCTION_NAME, []).

quorum_queue(Config) ->
    ok = queue(Config, ?FUNCTION_NAME, [{<<"x-queue-type">>, longstr, <<"quorum">>}]).

queue(Config, QName0, Args) ->
    QName1 = rabbit_data_coercion:to_binary(QName0),
    QName = <<QName1/binary, ?UNICODE_STRING/utf8>>,
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                           durable   = true,
                                           arguments = Args
                                          }),
    rabbit_ct_client_helpers:publish(Ch, QName, 1),
    {#'basic.get_ok'{}, #amqp_msg{payload = <<"1">>}} =
        amqp_channel:call(Ch, #'basic.get'{queue = QName, no_ack = false}),
    {'queue.delete_ok', 0} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok.

stream(Config) ->
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, stream_queue),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ConsumerTag = QName0 = atom_to_binary(?FUNCTION_NAME),
    QName = <<QName0/binary, ?UNICODE_STRING/utf8>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                           durable   = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]
                                          }),
    rabbit_ct_client_helpers:publish(Ch, QName, 1),
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                    prefetch_count = 1})),
    amqp_channel:subscribe(Ch,
                           #'basic.consume'{queue = QName,
                                            no_ack = false,
                                            consumer_tag = ConsumerTag,
                                            arguments = [{<<"x-stream-offset">>, long, 0}]},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = ConsumerTag} ->
            ok
    end,
    DelTag = receive
                 {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
                     DeliveryTag
             after 5000 ->
                       ct:fail(timeout)
             end,
    ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DelTag,
                                            multiple = false}),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = ConsumerTag}),
    {'queue.delete_ok', 0} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok.
