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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.FederationStatusCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.RestartFederationLinkCommand.erl",
            "src/rabbit_federation_app.erl",
            "src/rabbit_federation_db.erl",
            "src/rabbit_federation_event.erl",
            "src/rabbit_federation_exchange.erl",
            "src/rabbit_federation_exchange_link.erl",
            "src/rabbit_federation_exchange_link_sup_sup.erl",
            "src/rabbit_federation_link_sup.erl",
            "src/rabbit_federation_link_util.erl",
            "src/rabbit_federation_parameters.erl",
            "src/rabbit_federation_pg.erl",
            "src/rabbit_federation_queue.erl",
            "src/rabbit_federation_queue_link.erl",
            "src/rabbit_federation_queue_link_sup_sup.erl",
            "src/rabbit_federation_status.erl",
            "src/rabbit_federation_sup.erl",
            "src/rabbit_federation_upstream.erl",
            "src/rabbit_federation_upstream_exchange.erl",
            "src/rabbit_federation_util.erl",
            "src/rabbit_log_federation.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_federation",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit:erlang_app",
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
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.FederationStatusCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.RestartFederationLinkCommand.erl",
            "src/rabbit_federation_app.erl",
            "src/rabbit_federation_db.erl",
            "src/rabbit_federation_event.erl",
            "src/rabbit_federation_exchange.erl",
            "src/rabbit_federation_exchange_link.erl",
            "src/rabbit_federation_exchange_link_sup_sup.erl",
            "src/rabbit_federation_link_sup.erl",
            "src/rabbit_federation_link_util.erl",
            "src/rabbit_federation_parameters.erl",
            "src/rabbit_federation_pg.erl",
            "src/rabbit_federation_queue.erl",
            "src/rabbit_federation_queue_link.erl",
            "src/rabbit_federation_queue_link_sup_sup.erl",
            "src/rabbit_federation_status.erl",
            "src/rabbit_federation_sup.erl",
            "src/rabbit_federation_upstream.erl",
            "src/rabbit_federation_upstream_exchange.erl",
            "src/rabbit_federation_util.erl",
            "src/rabbit_log_federation.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_federation",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit:erlang_app",
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
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.FederationStatusCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.RestartFederationLinkCommand.erl",
            "src/rabbit_federation_app.erl",
            "src/rabbit_federation_db.erl",
            "src/rabbit_federation_event.erl",
            "src/rabbit_federation_exchange.erl",
            "src/rabbit_federation_exchange_link.erl",
            "src/rabbit_federation_exchange_link_sup_sup.erl",
            "src/rabbit_federation_link_sup.erl",
            "src/rabbit_federation_link_util.erl",
            "src/rabbit_federation_parameters.erl",
            "src/rabbit_federation_pg.erl",
            "src/rabbit_federation_queue.erl",
            "src/rabbit_federation_queue_link.erl",
            "src/rabbit_federation_queue_link_sup_sup.erl",
            "src/rabbit_federation_status.erl",
            "src/rabbit_federation_sup.erl",
            "src/rabbit_federation_upstream.erl",
            "src/rabbit_federation_upstream_exchange.erl",
            "src/rabbit_federation_util.erl",
            "src/rabbit_log_federation.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = [
            "include/logging.hrl",
            "include/rabbit_federation.hrl",
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
        name = "exchange_SUITE_beam_files",
        testonly = True,
        srcs = ["test/exchange_SUITE.erl"],
        outs = ["test/exchange_SUITE.beam"],
        hdrs = ["include/rabbit_federation.hrl"],
        app_name = "rabbitmq_federation",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
    erlang_bytecode(
        name = "federation_status_command_SUITE_beam_files",
        testonly = True,
        srcs = ["test/federation_status_command_SUITE.erl"],
        outs = ["test/federation_status_command_SUITE.beam"],
        app_name = "rabbitmq_federation",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "queue_SUITE_beam_files",
        testonly = True,
        srcs = ["test/queue_SUITE.erl"],
        outs = ["test/queue_SUITE.beam"],
        app_name = "rabbitmq_federation",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_federation_status_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_federation_status_SUITE.erl"],
        outs = ["test/rabbit_federation_status_SUITE.beam"],
        hdrs = ["include/rabbit_federation.hrl"],
        app_name = "rabbitmq_federation",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "restart_federation_link_command_SUITE_beam_files",
        testonly = True,
        srcs = ["test/restart_federation_link_command_SUITE.erl"],
        outs = ["test/restart_federation_link_command_SUITE.beam"],
        app_name = "rabbitmq_federation",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "test_rabbit_federation_test_util_beam",
        testonly = True,
        srcs = ["test/rabbit_federation_test_util.erl"],
        outs = ["test/rabbit_federation_test_util.beam"],
        hdrs = ["include/rabbit_federation.hrl"],
        app_name = "rabbitmq_federation",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "unit_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_SUITE.erl"],
        outs = ["test/unit_SUITE.beam"],
        hdrs = ["include/rabbit_federation.hrl"],
        app_name = "rabbitmq_federation",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "unit_inbroker_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_inbroker_SUITE.erl"],
        outs = ["test/unit_inbroker_SUITE.beam"],
        hdrs = ["include/rabbit_federation.hrl"],
        app_name = "rabbitmq_federation",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
