-module(client_id_interceptor_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(util,
        [connect/2]).

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
    Config.
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
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    ClientId = Container = atom_to_binary(?FUNCTION_NAME),

    %% With AMQP 1.0
    OpnConf = #{address => Host,
                port => Port,
                container_id => Container,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection1} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session(Connection1),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session1, <<"pair">>),
    QName = <<"queue for AMQP 1.0 client">>,
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, <<"amq.topic">>, <<"topic.1">>, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session1, <<"test-receiver">>,
                       rabbitmq_amqp_address:queue(QName),
                       unsettled, configuration),

    C = connect(ClientId, Config),
    Correlation = <<"some correlation ID">>,
    ContentType = <<"text/plain">>,
    RequestPayload = <<"my request">>,
    {ok, _} = emqtt:publish(C, <<"topic/1">>,
                            #{'Content-Type' => ContentType,
                              'Correlation-Data' => Correlation},
                            RequestPayload, [{qos, 1}]),

    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    Props = amqp10_msg:message_annotations(Msg1),
    ?assertMatch(ClientId, maps:get(<<"x-client_id">>, Props)),

    % With AMQP 0.9
    Ch = rabbit_ct_client_helpers:open_channel(Config),

    ?_assertMatch({#'basic.get_ok'{},
                   #amqp_msg{props = #'P_basic'{headers = [{<<"x-client_id">>,
                                                            longstr,
                                                            <<"incoming">>}]} }},
                  amqp_channel:call(Ch, #'basic.get'{queue = QName, no_ack = true})),

    rabbit_ct_client_helpers:close_channel(Ch),
    emqtt:disconnect(C).
