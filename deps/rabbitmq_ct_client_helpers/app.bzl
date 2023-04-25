load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        testonly = True,
        srcs = [":other_beam"],
    )
    erlang_bytecode(
        name = "other_beam",
        testonly = True,
        srcs = ["src/rabbit_ct_client_helpers.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_ct_client_helpers",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )

def all_srcs(name = "all_srcs"):
    filegroup(
        name = "all_srcs",
        testonly = True,
        srcs = [":public_and_private_hdrs", ":srcs"],
    )
    filegroup(
        name = "public_and_private_hdrs",
        testonly = True,
        srcs = [":private_hdrs", ":public_hdrs"],
    )

    filegroup(
        name = "priv",
        testonly = True,
    )

    filegroup(
        name = "srcs",
        testonly = True,
        srcs = ["src/rabbit_ct_client_helpers.erl"],
    )
    filegroup(
        name = "private_hdrs",
        testonly = True,
    )
    filegroup(
        name = "public_hdrs",
        testonly = True,
    )
    filegroup(
        name = "license_files",
        testonly = True,
        srcs = [
            "LICENSE",
            "LICENSE-MPL-RabbitMQ",
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
        srcs = ["src/rabbit_ct_client_helpers.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_ct_client_helpers",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    pass
