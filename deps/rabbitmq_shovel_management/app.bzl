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
            "src/rabbit_shovel_mgmt.erl",
            "src/rabbit_shovel_mgmt_util.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_shovel_management",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbitmq_management:erlang_app",
            "//deps/rabbitmq_management_agent:erlang_app",
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
            "src/rabbit_shovel_mgmt.erl",
            "src/rabbit_shovel_mgmt_util.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_shovel_management",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbitmq_management:erlang_app",
            "//deps/rabbitmq_management_agent:erlang_app",
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
        name = "public_hdrs",
    )

    filegroup(
        name = "priv",
        srcs = [
            "priv/www/js/shovel.js",
            "priv/www/js/tmpl/dynamic-shovel.ejs",
            "priv/www/js/tmpl/dynamic-shovels.ejs",
            "priv/www/js/tmpl/shovels.ejs",
        ],
    )
    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbit_shovel_mgmt.erl",
            "src/rabbit_shovel_mgmt_util.erl",
        ],
    )
    filegroup(
        name = "private_hdrs",
        srcs = ["src/rabbit_shovel_mgmt.hrl"],
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
        name = "http_SUITE_beam_files",
        testonly = True,
        srcs = ["test/http_SUITE.erl"],
        outs = ["test/http_SUITE.beam"],
        app_name = "rabbitmq_shovel_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_shovel_mgmt_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_shovel_mgmt_SUITE.erl"],
        outs = ["test/rabbit_shovel_mgmt_SUITE.beam"],
        app_name = "rabbitmq_shovel_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app", "//deps/rabbitmq_management_agent:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_shovel_mgmt_util_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_shovel_mgmt_util_SUITE.erl"],
        outs = ["test/rabbit_shovel_mgmt_util_SUITE.beam"],
        app_name = "rabbitmq_shovel_management",
        erlc_opts = "//:test_erlc_opts",
    )
