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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.AddSuperStreamCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteSuperStreamCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConnectionsCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumerGroupsCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamGroupConsumersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamPublishersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamTrackingCommand.erl",
            "src/rabbit_stream.erl",
            "src/rabbit_stream_connection_sup.erl",
            "src/rabbit_stream_manager.erl",
            "src/rabbit_stream_metrics.erl",
            "src/rabbit_stream_metrics_gc.erl",
            "src/rabbit_stream_reader.erl",
            "src/rabbit_stream_sup.erl",
            "src/rabbit_stream_utils.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_stream",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "//deps/rabbitmq_stream_common:erlang_app",
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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.AddSuperStreamCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteSuperStreamCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConnectionsCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumerGroupsCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamGroupConsumersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamPublishersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamTrackingCommand.erl",
            "src/rabbit_stream.erl",
            "src/rabbit_stream_connection_sup.erl",
            "src/rabbit_stream_manager.erl",
            "src/rabbit_stream_metrics.erl",
            "src/rabbit_stream_metrics_gc.erl",
            "src/rabbit_stream_reader.erl",
            "src/rabbit_stream_sup.erl",
            "src/rabbit_stream_utils.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_stream",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "//deps/rabbitmq_stream_common:erlang_app",
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
        srcs = ["priv/schema/rabbitmq_stream.schema"],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "srcs",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.AddSuperStreamCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteSuperStreamCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConnectionsCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumerGroupsCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamGroupConsumersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamPublishersCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamTrackingCommand.erl",
            "src/rabbit_stream.erl",
            "src/rabbit_stream_connection_sup.erl",
            "src/rabbit_stream_manager.erl",
            "src/rabbit_stream_metrics.erl",
            "src/rabbit_stream_metrics_gc.erl",
            "src/rabbit_stream_reader.erl",
            "src/rabbit_stream_sup.erl",
            "src/rabbit_stream_utils.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/rabbit_stream_metrics.hrl"],
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
        name = "commands_SUITE_beam_files",
        testonly = True,
        srcs = ["test/commands_SUITE.erl"],
        outs = ["test/commands_SUITE.beam"],
        app_name = "rabbitmq_stream",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app", "//deps/rabbitmq_stream_common:erlang_app"],
    )
    erlang_bytecode(
        name = "config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_schema_SUITE.erl"],
        outs = ["test/config_schema_SUITE.beam"],
        app_name = "rabbitmq_stream",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "rabbit_stream_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_stream_SUITE.erl"],
        outs = ["test/rabbit_stream_SUITE.beam"],
        hdrs = ["include/rabbit_stream_metrics.hrl"],
        app_name = "rabbitmq_stream",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit:erlang_app", "//deps/rabbit_common:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app", "//deps/rabbitmq_stream_common:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_stream_manager_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_stream_manager_SUITE.erl"],
        outs = ["test/rabbit_stream_manager_SUITE.beam"],
        app_name = "rabbitmq_stream",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_stream_utils_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_stream_utils_SUITE.erl"],
        outs = ["test/rabbit_stream_utils_SUITE.beam"],
        app_name = "rabbitmq_stream",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
