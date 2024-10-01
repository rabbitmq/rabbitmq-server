load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":other_beam"],
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListWebMqttConnectionsCommand.erl",
            "src/rabbit_web_mqtt_app.erl",
            "src/rabbit_web_mqtt_handler.erl",
            "src/rabbit_web_mqtt_stream_handler.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_mqtt",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "//deps/rabbitmq_mqtt:erlang_app",
            "@cowboy//:erlang_app",
        ],
    )

def all_test_beam_files(name = "all_test_beam_files"):
    filegroup(
        name = "test_beam_files",
        testonly = True,
        srcs = [":test_other_beam"],
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListWebMqttConnectionsCommand.erl",
            "src/rabbit_web_mqtt_app.erl",
            "src/rabbit_web_mqtt_handler.erl",
            "src/rabbit_web_mqtt_stream_handler.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_mqtt",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "//deps/rabbitmq_mqtt:erlang_app",
            "@cowboy//:erlang_app",
        ],
    )

def all_srcs(name = "all_srcs"):
    filegroup(
        name = "all_srcs",
        srcs = [":public_and_private_hdrs", ":srcs"],
    )
    filegroup(
        name = "public_and_private_hdrs",
        srcs = [":private_hdrs", ":public_hdrs"],
    )

    filegroup(
        name = "priv",
        srcs = ["priv/schema/rabbitmq_web_mqtt.schema"],
    )
    filegroup(
        name = "public_hdrs",
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListWebMqttConnectionsCommand.erl",
            "src/rabbit_web_mqtt_app.erl",
            "src/rabbit_web_mqtt_handler.erl",
            "src/rabbit_web_mqtt_stream_handler.erl",
        ],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "license_files",
        srcs = [
            "LICENSE",
            "LICENSE-MPL-RabbitMQ",
        ],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "test_src_rabbit_ws_test_util_beam",
        testonly = True,
        srcs = ["test/src/rabbit_ws_test_util.erl"],
        outs = ["test/src/rabbit_ws_test_util.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_src_rfc6455_client_beam",
        testonly = True,
        srcs = ["test/src/rfc6455_client.erl"],
        outs = ["test/src/rfc6455_client.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )

    erlang_bytecode(
        name = "test_rabbit_web_mqtt_test_util_beam",
        testonly = True,
        srcs = ["test/rabbit_web_mqtt_test_util.erl"],
        outs = ["test/rabbit_web_mqtt_test_util.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "web_mqtt_command_SUITE_beam_files",
        testonly = True,
        srcs = ["test/web_mqtt_command_SUITE.erl"],
        outs = ["test/web_mqtt_command_SUITE.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_mqtt:erlang_app"],
    )
    erlang_bytecode(
        name = "web_mqtt_config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/web_mqtt_config_schema_SUITE.erl"],
        outs = ["test/web_mqtt_config_schema_SUITE.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "web_mqtt_proxy_protocol_SUITE_beam_files",
        testonly = True,
        srcs = ["test/web_mqtt_proxy_protocol_SUITE.erl"],
        outs = ["test/web_mqtt_proxy_protocol_SUITE.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "web_mqtt_shared_SUITE_beam_files",
        testonly = True,
        srcs = ["test/web_mqtt_shared_SUITE.erl"],
        outs = ["test/web_mqtt_shared_SUITE.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "web_mqtt_system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/web_mqtt_system_SUITE.erl"],
        outs = ["test/web_mqtt_system_SUITE.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "web_mqtt_v5_SUITE_beam_files",
        testonly = True,
        srcs = ["test/web_mqtt_v5_SUITE.erl"],
        outs = ["test/web_mqtt_v5_SUITE.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
