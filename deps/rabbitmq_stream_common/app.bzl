load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":other_beam"],
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = ["src/rabbit_stream_core.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_stream_common",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
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
        srcs = ["src/rabbit_stream_core.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_stream_common",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
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
        srcs = ["src/rabbit_stream_core.erl"],
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/rabbit_stream.hrl"],
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
        name = "rabbit_stream_core_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_stream_core_SUITE.erl"],
        outs = ["test/rabbit_stream_core_SUITE.beam"],
        hdrs = ["include/rabbit_stream.hrl"],
        app_name = "rabbitmq_stream_common",
        erlc_opts = "//:test_erlc_opts",
    )
