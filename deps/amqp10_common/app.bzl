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
            "src/amqp10_binary_generator.erl",
            "src/amqp10_binary_parser.erl",
            "src/amqp10_framing.erl",
            "src/amqp10_framing0.erl",
            "src/amqp10_util.erl",
            "src/serial_number.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "amqp10_common",
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
        srcs = [
            "src/amqp10_binary_generator.erl",
            "src/amqp10_binary_parser.erl",
            "src/amqp10_framing.erl",
            "src/amqp10_framing0.erl",
            "src/amqp10_util.erl",
            "src/serial_number.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "amqp10_common",
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
        srcs = [
            "src/amqp10_binary_generator.erl",
            "src/amqp10_binary_parser.erl",
            "src/amqp10_framing.erl",
            "src/amqp10_framing0.erl",
            "src/amqp10_util.erl",
            "src/serial_number.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/amqp10_filtex.hrl", "include/amqp10_framing.hrl", "include/amqp10_types.hrl"],
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
        name = "binary_generator_SUITE_beam_files",
        testonly = True,
        srcs = ["test/binary_generator_SUITE.erl"],
        outs = ["test/binary_generator_SUITE.beam"],
        app_name = "amqp10_common",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "binary_parser_SUITE_beam_files",
        testonly = True,
        srcs = ["test/binary_parser_SUITE.erl"],
        outs = ["test/binary_parser_SUITE.beam"],
        app_name = "amqp10_common",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "serial_number_SUITE_beam_files",
        testonly = True,
        srcs = ["test/serial_number_SUITE.erl"],
        outs = ["test/serial_number_SUITE.beam"],
        app_name = "amqp10_common",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "prop_SUITE_beam_files",
        testonly = True,
        srcs = ["test/prop_SUITE.erl"],
        outs = ["test/prop_SUITE.beam"],
        hdrs = ["include/amqp10_framing.hrl"],
        app_name = "amqp10_common",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
