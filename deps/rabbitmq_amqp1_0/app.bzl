load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":other_beam"],
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = ["src/rabbitmq_amqp1_0_noop.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_amqp1_0",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
    )

def all_srcs(name = "all_srcs"):
    filegroup(
        name = "srcs",
        srcs = ["src/rabbitmq_amqp1_0_noop.erl"],
    )
    filegroup(name = "private_hdrs")
    filegroup(name = "public_hdrs")
    filegroup(name = "priv")
    filegroup(name = "license_files")
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
        srcs = ["src/rabbitmq_amqp1_0_noop.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_amqp1_0",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
    )
    filegroup(
        name = "test_beam_files",
        testonly = True,
        srcs = [":test_other_beam"],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    pass
