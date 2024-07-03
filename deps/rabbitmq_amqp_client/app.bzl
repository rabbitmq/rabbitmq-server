load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":other_beam"],
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = ["src/rabbitmq_amqp_address.erl", "src/rabbitmq_amqp_client.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_amqp_client",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )

def all_srcs(name = "all_srcs"):
    filegroup(
        name = "srcs",
        srcs = ["src/rabbitmq_amqp_address.erl", "src/rabbitmq_amqp_client.erl"],
    )
    filegroup(name = "private_hdrs")
    filegroup(
        name = "public_hdrs",
        srcs = ["include/rabbitmq_amqp_client.hrl"],
    )
    filegroup(name = "priv")
    filegroup(
        name = "license_files",
        srcs = [
            "LICENSE",
            "LICENSE-MPL-RabbitMQ",
        ],
    )
    filegroup(
        name = "public_and_private_hdrs",
        srcs = [":private_hdrs", ":public_hdrs"],
    )
    filegroup(
        name = "all_srcs",
        srcs = [":public_and_private_hdrs", ":srcs"],
    )

def all_test_beam_files(name = "all_test_beam_files"):
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = ["src/rabbitmq_amqp_address.erl", "src/rabbitmq_amqp_client.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_amqp_client",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    filegroup(
        name = "test_beam_files",
        testonly = True,
        srcs = [":test_other_beam"],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "management_SUITE_beam_files",
        testonly = True,
        srcs = ["test/management_SUITE.erl"],
        outs = ["test/management_SUITE.beam"],
        hdrs = ["include/rabbitmq_amqp_client.hrl"],
        app_name = "rabbitmq_amqp_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app", "//deps/rabbit_common:erlang_app"],
    )
