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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListAmqp10ConnectionsCommand.erl",
            "src/rabbit_amqp1_0.erl",
            "src/rabbit_amqp1_0_channel.erl",
            "src/rabbit_amqp1_0_incoming_link.erl",
            "src/rabbit_amqp1_0_link_util.erl",
            "src/rabbit_amqp1_0_message.erl",
            "src/rabbit_amqp1_0_outgoing_link.erl",
            "src/rabbit_amqp1_0_reader.erl",
            "src/rabbit_amqp1_0_session.erl",
            "src/rabbit_amqp1_0_session_process.erl",
            "src/rabbit_amqp1_0_session_sup.erl",
            "src/rabbit_amqp1_0_session_sup_sup.erl",
            "src/rabbit_amqp1_0_util.erl",
            "src/rabbit_amqp1_0_writer.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_amqp1_0",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp10_common:erlang_app",
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListAmqp10ConnectionsCommand.erl",
            "src/rabbit_amqp1_0.erl",
            "src/rabbit_amqp1_0_channel.erl",
            "src/rabbit_amqp1_0_incoming_link.erl",
            "src/rabbit_amqp1_0_link_util.erl",
            "src/rabbit_amqp1_0_message.erl",
            "src/rabbit_amqp1_0_outgoing_link.erl",
            "src/rabbit_amqp1_0_reader.erl",
            "src/rabbit_amqp1_0_session.erl",
            "src/rabbit_amqp1_0_session_process.erl",
            "src/rabbit_amqp1_0_session_sup.erl",
            "src/rabbit_amqp1_0_session_sup_sup.erl",
            "src/rabbit_amqp1_0_util.erl",
            "src/rabbit_amqp1_0_writer.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_amqp1_0",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp10_common:erlang_app",
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
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
        srcs = ["priv/schema/rabbitmq_amqp1_0.schema"],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "srcs",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListAmqp10ConnectionsCommand.erl",
            "src/rabbit_amqp1_0.erl",
            "src/rabbit_amqp1_0_channel.erl",
            "src/rabbit_amqp1_0_incoming_link.erl",
            "src/rabbit_amqp1_0_link_util.erl",
            "src/rabbit_amqp1_0_message.erl",
            "src/rabbit_amqp1_0_outgoing_link.erl",
            "src/rabbit_amqp1_0_reader.erl",
            "src/rabbit_amqp1_0_session.erl",
            "src/rabbit_amqp1_0_session_process.erl",
            "src/rabbit_amqp1_0_session_sup.erl",
            "src/rabbit_amqp1_0_session_sup_sup.erl",
            "src/rabbit_amqp1_0_util.erl",
            "src/rabbit_amqp1_0_writer.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/rabbit_amqp1_0.hrl"],
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
        name = "amqp10_client_SUITE_beam_files",
        testonly = True,
        srcs = ["test/amqp10_client_SUITE.erl"],
        outs = ["test/amqp10_client_SUITE.beam"],
        app_name = "rabbitmq_amqp1_0",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "command_SUITE_beam_files",
        testonly = True,
        srcs = ["test/command_SUITE.erl"],
        outs = ["test/command_SUITE.beam"],
        hdrs = ["include/rabbit_amqp1_0.hrl"],
        app_name = "rabbitmq_amqp1_0",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app", "//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_schema_SUITE.erl"],
        outs = ["test/config_schema_SUITE.beam"],
        app_name = "rabbitmq_amqp1_0",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "proxy_protocol_SUITE_beam_files",
        testonly = True,
        srcs = ["test/proxy_protocol_SUITE.erl"],
        outs = ["test/proxy_protocol_SUITE.beam"],
        app_name = "rabbitmq_amqp1_0",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/system_SUITE.erl"],
        outs = ["test/system_SUITE.beam"],
        app_name = "rabbitmq_amqp1_0",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "unit_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_SUITE.erl"],
        outs = ["test/unit_SUITE.beam"],
        hdrs = ["include/rabbit_amqp1_0.hrl"],
        app_name = "rabbitmq_amqp1_0",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
