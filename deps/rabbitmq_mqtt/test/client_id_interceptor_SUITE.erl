-module(client_id_interceptor_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(util,
        [connect/3]).

all() ->
    [{group, intercept}].

groups() ->
    [
     {intercept, [], [incoming]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config0, [{rmq_nodename_suffix, Testcase}]),
    Val = maps:to_list(
            maps:from_keys([rabbit_mqtt_message_interceptor_client_id],
                           #{annotation_key => <<"x-client_id">>})),
    Config2 = rabbit_ct_helpers:merge_app_env(
               Config1, {rabbit, [{incoming_message_interceptors, Val}]}),
    Config3 = rabbit_ct_helpers:run_steps(
      Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++
      [fun start_amqp10_client_app/1]),
    rabbit_ct_helpers:testcase_started(Config3, Testcase).

end_per_testcase(Testcase, Config0) ->
    Config = rabbit_ct_helpers:testcase_finished(Config0, Testcase),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

start_amqp10_client_app(Config) ->
    ?assertMatch({ok, _}, application:ensure_all_started(amqp10_client)),
    Config.

incoming(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),

    QQ = <<"qq1">>,
    Topic = <<"mytopic">>,

    declare_queue(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    bind(Ch, QQ, Topic),

    ClientId = ?FUNCTION_NAME,
    C = connect(ClientId, Config, [{max_inflight, 200},
                                   {retry_interval, 2}]),

    ?_assertMatch({ok, _}, emqtt:publish(C, Topic, <<"1">>, [{qos, 1}])),
    ?_assertMatch({#'basic.get_ok'{},
                   #amqp_msg{props = #'P_basic'{headers = [{<<"x-client_id">>,
                                                            longstr,
                                                            <<"incoming">>}]} }},
                  amqp_channel:call(Ch, #'basic.get'{queue = QQ, no_ack = true})),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver">>, <<"qq1">>),

    receive M ->
        ct:log("Received message: ~p", [M]),
        ok
    after 5000 ->
        ct:log("Timeout waiting for message")
    end,

    %% grant some credit to the remote sender but don't auto-renew it
    ok = amqp10_client:flow_link_credit(Receiver, 5, never),

    %% wait for a delivery
    receive
        {amqp10_msg, Receiver, InMsg} ->
            ct:log("Received message: ~p", [InMsg]),
            ok
    after 2000 ->
          exit(delivery_timeout)
    end.


declare_queue(Ch, QueueName, Args)
  when is_pid(Ch), is_binary(QueueName), is_list(Args) ->
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QueueName,
                                     durable = true,
                                     arguments = Args}).

bind(Ch, QueueName, Topic)
  when is_pid(Ch), is_binary(QueueName), is_binary(Topic) ->
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue       = QueueName,
                                             exchange    = <<"amq.topic">>,
                                             routing_key = Topic}).

connection_config(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>}}.
