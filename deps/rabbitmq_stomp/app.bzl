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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStompConnectionsCommand.erl",
            "src/rabbit_stomp.erl",
            "src/rabbit_stomp_client_sup.erl",
            "src/rabbit_stomp_connection_info.erl",
            "src/rabbit_stomp_frame.erl",
            "src/rabbit_stomp_internal_event_handler.erl",
            "src/rabbit_stomp_processor.erl",
            "src/rabbit_stomp_reader.erl",
            "src/rabbit_stomp_sup.erl",
            "src/rabbit_stomp_util.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_stomp",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStompConnectionsCommand.erl",
            "src/rabbit_stomp.erl",
            "src/rabbit_stomp_client_sup.erl",
            "src/rabbit_stomp_connection_info.erl",
            "src/rabbit_stomp_frame.erl",
            "src/rabbit_stomp_internal_event_handler.erl",
            "src/rabbit_stomp_processor.erl",
            "src/rabbit_stomp_reader.erl",
            "src/rabbit_stomp_sup.erl",
            "src/rabbit_stomp_util.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_stomp",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
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
        srcs = ["priv/schema/rabbitmq_stomp.schema"],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "srcs",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStompConnectionsCommand.erl",
            "src/rabbit_stomp.erl",
            "src/rabbit_stomp_client_sup.erl",
            "src/rabbit_stomp_connection_info.erl",
            "src/rabbit_stomp_frame.erl",
            "src/rabbit_stomp_internal_event_handler.erl",
            "src/rabbit_stomp_processor.erl",
            "src/rabbit_stomp_reader.erl",
            "src/rabbit_stomp_sup.erl",
            "src/rabbit_stomp_util.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = [
            "include/rabbit_stomp.hrl",
            "include/rabbit_stomp_frame.hrl",
            "include/rabbit_stomp_headers.hrl",
        ],
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
        name = "command_SUITE_beam_files",
        testonly = True,
        srcs = ["test/command_SUITE.erl"],
        outs = ["test/command_SUITE.beam"],
        hdrs = ["include/rabbit_stomp.hrl"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_schema_SUITE.erl"],
        outs = ["test/config_schema_SUITE.beam"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "connections_SUITE_beam_files",
        testonly = True,
        srcs = ["test/connections_SUITE.erl"],
        outs = ["test/connections_SUITE.beam"],
        hdrs = ["include/rabbit_stomp_frame.hrl"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "frame_SUITE_beam_files",
        testonly = True,
        srcs = ["test/frame_SUITE.erl"],
        outs = ["test/frame_SUITE.beam"],
        hdrs = ["include/rabbit_stomp_frame.hrl", "include/rabbit_stomp_headers.hrl"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "proxy_protocol_SUITE_beam_files",
        testonly = True,
        srcs = ["test/proxy_protocol_SUITE.erl"],
        outs = ["test/proxy_protocol_SUITE.beam"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "python_SUITE_beam_files",
        testonly = True,
        srcs = ["test/python_SUITE.erl"],
        outs = ["test/python_SUITE.beam"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/system_SUITE.erl"],
        outs = ["test/system_SUITE.beam"],
        hdrs = ["include/rabbit_stomp.hrl", "include/rabbit_stomp_frame.hrl", "include/rabbit_stomp_headers.hrl"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "test_src_rabbit_stomp_client_beam",
        testonly = True,
        srcs = ["test/src/rabbit_stomp_client.erl"],
        outs = ["test/src/rabbit_stomp_client.beam"],
        hdrs = ["include/rabbit_stomp_frame.hrl"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_src_rabbit_stomp_publish_test_beam",
        testonly = True,
        srcs = ["test/src/rabbit_stomp_publish_test.erl"],
        outs = ["test/src/rabbit_stomp_publish_test.beam"],
        hdrs = ["include/rabbit_stomp_frame.hrl"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "topic_SUITE_beam_files",
        testonly = True,
        srcs = ["test/topic_SUITE.erl"],
        outs = ["test/topic_SUITE.beam"],
        hdrs = ["include/rabbit_stomp.hrl", "include/rabbit_stomp_frame.hrl", "include/rabbit_stomp_headers.hrl"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "util_SUITE_beam_files",
        testonly = True,
        srcs = ["test/util_SUITE.erl"],
        outs = ["test/util_SUITE.beam"],
        hdrs = ["include/rabbit_stomp_frame.hrl"],
        app_name = "rabbitmq_stomp",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
