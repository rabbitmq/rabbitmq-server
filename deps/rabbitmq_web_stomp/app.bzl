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
            "src/rabbit_web_stomp_app.erl",
            "src/rabbit_web_stomp_connection_sup.erl",
            "src/rabbit_web_stomp_handler.erl",
            "src/rabbit_web_stomp_internal_event_handler.erl",
            "src/rabbit_web_stomp_listener.erl",
            "src/rabbit_web_stomp_middleware.erl",
            "src/rabbit_web_stomp_stream_handler.erl",
            "src/rabbit_web_stomp_sup.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_stomp",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_stomp:erlang_app",
            "@cowboy//:erlang_app",
            "@ranch//:erlang_app",
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
            "src/rabbit_web_stomp_app.erl",
            "src/rabbit_web_stomp_connection_sup.erl",
            "src/rabbit_web_stomp_handler.erl",
            "src/rabbit_web_stomp_internal_event_handler.erl",
            "src/rabbit_web_stomp_listener.erl",
            "src/rabbit_web_stomp_middleware.erl",
            "src/rabbit_web_stomp_stream_handler.erl",
            "src/rabbit_web_stomp_sup.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_stomp",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_stomp:erlang_app",
            "@cowboy//:erlang_app",
            "@ranch//:erlang_app",
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
        srcs = ["priv/schema/rabbitmq_web_stomp.schema"],
    )
    filegroup(
        name = "public_hdrs",
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbit_web_stomp_app.erl",
            "src/rabbit_web_stomp_connection_sup.erl",
            "src/rabbit_web_stomp_handler.erl",
            "src/rabbit_web_stomp_internal_event_handler.erl",
            "src/rabbit_web_stomp_listener.erl",
            "src/rabbit_web_stomp_middleware.erl",
            "src/rabbit_web_stomp_stream_handler.erl",
            "src/rabbit_web_stomp_sup.erl",
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
        name = "amqp_stomp_SUITE_beam_files",
        testonly = True,
        srcs = ["test/amqp_stomp_SUITE.erl"],
        outs = ["test/amqp_stomp_SUITE.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_schema_SUITE.erl"],
        outs = ["test/config_schema_SUITE.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "cowboy_websocket_SUITE_beam_files",
        testonly = True,
        srcs = ["test/cowboy_websocket_SUITE.erl"],
        outs = ["test/cowboy_websocket_SUITE.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "proxy_protocol_SUITE_beam_files",
        testonly = True,
        srcs = ["test/proxy_protocol_SUITE.erl"],
        outs = ["test/proxy_protocol_SUITE.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "raw_websocket_SUITE_beam_files",
        testonly = True,
        srcs = ["test/raw_websocket_SUITE.erl"],
        outs = ["test/raw_websocket_SUITE.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_src_rabbit_ws_test_util_beam",
        testonly = True,
        srcs = ["test/src/rabbit_ws_test_util.erl"],
        outs = ["test/src/rabbit_ws_test_util.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_src_rfc6455_client_beam",
        testonly = True,
        srcs = ["test/src/rfc6455_client.erl"],
        outs = ["test/src/rfc6455_client.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_src_stomp_beam",
        testonly = True,
        srcs = ["test/src/stomp.erl"],
        outs = ["test/src/stomp.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "unit_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_SUITE.erl"],
        outs = ["test/unit_SUITE.beam"],
        app_name = "rabbitmq_web_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
