-module(python_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

all() ->
    [
    common,
    ssl,
    connect_options
    ].


init_per_testcase(_, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_certspwd, "bunnychow"}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
        Config1, 
        rabbit_ct_broker_helpers:setup_steps()).

end_per_testcase(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).


common(Config) ->
    run(Config, "/src/test.py").

connect_options(Config) ->
    run(Config, "/src/test_connect_options.py").

ssl(Config) ->
    run(Config, "/src/test_ssl.py").

run(Config, Test) ->
    Curdir = cur_dir(),
    CertsDir = rabbit_ct_helpers:get_config(Config, rmq_certsdir),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    StompPortTls = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp_tls),
    AmqpPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    NodeName = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    PythonPath = os:getenv("PYTHONPATH"),
    os:putenv("PYTHONPATH", Curdir ++ "/deps/pika/pika:"++ Curdir ++ "/deps/stomppy/stomppy:" ++ PythonPath),
    os:putenv("AMQP_PORT", integer_to_list(AmqpPort)),
    os:putenv("STOMP_PORT", integer_to_list(StompPort)),
    os:putenv("STOMP_PORT_TLS", integer_to_list(StompPortTls)),
    os:putenv("RABBITMQ_NODENAME", atom_to_list(NodeName)),
    os:putenv("SSL_CERTS_PATH", CertsDir),
    {ok, _} = rabbit_ct_helpers:exec([Curdir ++ Test]).


cur_dir() ->
    {Src, _} = filename:find_src(?MODULE),
    filename:dirname(Src).
