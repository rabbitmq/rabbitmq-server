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
            "src/Elixir.RabbitMQ.CLI.Diagnostics.Commands.ConsistentHashExchangeRingStateCommand.erl",
            "src/rabbit_db_ch_exchange.erl",
            "src/rabbit_db_ch_exchange_m2k_converter.erl",
            "src/rabbit_exchange_type_consistent_hash.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_consistent_hash_exchange",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "@khepri//:erlang_app",
            "@khepri_mnesia_migration//:erlang_app",
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
            "src/Elixir.RabbitMQ.CLI.Diagnostics.Commands.ConsistentHashExchangeRingStateCommand.erl",
            "src/rabbit_db_ch_exchange.erl",
            "src/rabbit_db_ch_exchange_m2k_converter.erl",
            "src/rabbit_exchange_type_consistent_hash.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_consistent_hash_exchange",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "@khepri//:erlang_app",
            "@khepri_mnesia_migration//:erlang_app",
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
            "src/Elixir.RabbitMQ.CLI.Diagnostics.Commands.ConsistentHashExchangeRingStateCommand.erl",
            "src/rabbit_db_ch_exchange.erl",
            "src/rabbit_db_ch_exchange_m2k_converter.erl",
            "src/rabbit_exchange_type_consistent_hash.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/rabbitmq_consistent_hash_exchange.hrl"],
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
        name = "rabbit_exchange_type_consistent_hash_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_exchange_type_consistent_hash_SUITE.erl"],
        outs = ["test/rabbit_exchange_type_consistent_hash_SUITE.beam"],
        hdrs = ["include/rabbitmq_consistent_hash_exchange.hrl"],
        app_name = "rabbitmq_consistent_hash_exchange",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
