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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ResetStatsDbCommand.erl",
            "src/exometer_slide.erl",
            "src/rabbit_mgmt_agent_app.erl",
            "src/rabbit_mgmt_agent_config.erl",
            "src/rabbit_mgmt_agent_sup.erl",
            "src/rabbit_mgmt_agent_sup_sup.erl",
            "src/rabbit_mgmt_data.erl",
            "src/rabbit_mgmt_data_compat.erl",
            "src/rabbit_mgmt_db_handler.erl",
            "src/rabbit_mgmt_external_stats.erl",
            "src/rabbit_mgmt_ff.erl",
            "src/rabbit_mgmt_format.erl",
            "src/rabbit_mgmt_gc.erl",
            "src/rabbit_mgmt_metrics_collector.erl",
            "src/rabbit_mgmt_metrics_gc.erl",
            "src/rabbit_mgmt_storage.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_management_agent",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "//deps/rabbitmq_web_dispatch:erlang_app",
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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ResetStatsDbCommand.erl",
            "src/exometer_slide.erl",
            "src/rabbit_mgmt_agent_app.erl",
            "src/rabbit_mgmt_agent_config.erl",
            "src/rabbit_mgmt_agent_sup.erl",
            "src/rabbit_mgmt_agent_sup_sup.erl",
            "src/rabbit_mgmt_data.erl",
            "src/rabbit_mgmt_data_compat.erl",
            "src/rabbit_mgmt_db_handler.erl",
            "src/rabbit_mgmt_external_stats.erl",
            "src/rabbit_mgmt_ff.erl",
            "src/rabbit_mgmt_format.erl",
            "src/rabbit_mgmt_gc.erl",
            "src/rabbit_mgmt_metrics_collector.erl",
            "src/rabbit_mgmt_metrics_gc.erl",
            "src/rabbit_mgmt_storage.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_management_agent",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "//deps/rabbitmq_web_dispatch:erlang_app",
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
        srcs = ["priv/schema/rabbitmq_management_agent.schema"],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "srcs",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ResetStatsDbCommand.erl",
            "src/exometer_slide.erl",
            "src/rabbit_mgmt_agent_app.erl",
            "src/rabbit_mgmt_agent_config.erl",
            "src/rabbit_mgmt_agent_sup.erl",
            "src/rabbit_mgmt_agent_sup_sup.erl",
            "src/rabbit_mgmt_data.erl",
            "src/rabbit_mgmt_data_compat.erl",
            "src/rabbit_mgmt_db_handler.erl",
            "src/rabbit_mgmt_external_stats.erl",
            "src/rabbit_mgmt_ff.erl",
            "src/rabbit_mgmt_format.erl",
            "src/rabbit_mgmt_gc.erl",
            "src/rabbit_mgmt_metrics_collector.erl",
            "src/rabbit_mgmt_metrics_gc.erl",
            "src/rabbit_mgmt_storage.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = [
            "include/rabbit_mgmt_agent.hrl",
            "include/rabbit_mgmt_metrics.hrl",
            "include/rabbit_mgmt_records.hrl",
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
        name = "exometer_slide_SUITE_beam_files",
        testonly = True,
        srcs = ["test/exometer_slide_SUITE.erl"],
        outs = ["test/exometer_slide_SUITE.beam"],
        app_name = "rabbitmq_management_agent",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "metrics_SUITE_beam_files",
        testonly = True,
        srcs = ["test/metrics_SUITE.erl"],
        outs = ["test/metrics_SUITE.beam"],
        app_name = "rabbitmq_management_agent",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_mgmt_gc_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_gc_SUITE.erl"],
        outs = ["test/rabbit_mgmt_gc_SUITE.beam"],
        hdrs = ["include/rabbit_mgmt_metrics.hrl"],
        app_name = "rabbitmq_management_agent",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_mgmt_slide_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_slide_SUITE.erl"],
        outs = ["test/rabbit_mgmt_slide_SUITE.beam"],
        app_name = "rabbitmq_management_agent",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
