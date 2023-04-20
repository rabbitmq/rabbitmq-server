load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":behaviours", ":other_beam"],
    )
    erlang_bytecode(
        name = "behaviours",
        srcs = ["src/rabbit_shovel_behaviour.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_shovel",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = native.glob(
            ["src/**/*.erl"],
            exclude = [
                "src/rabbit_shovel_behaviour.erl",
            ],
        ),
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_shovel",
        beam = [":behaviours"],
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
        srcs = [":test_behaviours", ":test_other_beam"],
    )
    erlang_bytecode(
        name = "test_behaviours",
        testonly = True,
        srcs = ["src/rabbit_shovel_behaviour.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_shovel",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = native.glob(
            ["src/**/*.erl"],
            exclude = [
                "src/rabbit_shovel_behaviour.erl",
            ],
        ),
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_shovel",
        beam = [":test_behaviours"],
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
        srcs = native.glob(["priv/**/*"]),
    )

    filegroup(
        name = "srcs",
        srcs = native.glob([
            "src/**/*.app.src",
            "src/**/*.erl",
        ]),
    )
    filegroup(
        name = "public_hdrs",
        srcs = native.glob(["include/**/*.hrl"]),
    )
    filegroup(
        name = "private_hdrs",
        srcs = native.glob(["src/**/*.hrl"]),
    )
    filegroup(
        name = "license_files",
        srcs = native.glob(["LICENSE*"]),
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "amqp10_SUITE_beam_files",
        testonly = True,
        srcs = ["test/amqp10_SUITE.erl"],
        outs = ["test/amqp10_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "amqp10_dynamic_SUITE_beam_files",
        testonly = True,
        srcs = ["test/amqp10_dynamic_SUITE.erl"],
        outs = ["test/amqp10_dynamic_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "amqp10_shovel_SUITE_beam_files",
        testonly = True,
        srcs = ["test/amqp10_shovel_SUITE.erl"],
        outs = ["test/amqp10_shovel_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "config_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_SUITE.erl"],
        outs = ["test/config_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "configuration_SUITE_beam_files",
        testonly = True,
        srcs = ["test/configuration_SUITE.erl"],
        outs = ["test/configuration_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "delete_shovel_command_SUITE_beam_files",
        testonly = True,
        srcs = ["test/delete_shovel_command_SUITE.erl"],
        outs = ["test/delete_shovel_command_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "dynamic_SUITE_beam_files",
        testonly = True,
        srcs = ["test/dynamic_SUITE.erl"],
        outs = ["test/dynamic_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "parameters_SUITE_beam_files",
        testonly = True,
        srcs = ["test/parameters_SUITE.erl"],
        outs = ["test/parameters_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "shovel_status_command_SUITE_beam_files",
        testonly = True,
        srcs = ["test/shovel_status_command_SUITE.erl"],
        outs = ["test/shovel_status_command_SUITE.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "test_shovel_test_utils_beam",
        testonly = True,
        srcs = ["test/shovel_test_utils.erl"],
        outs = ["test/shovel_test_utils.beam"],
        app_name = "rabbitmq_shovel",
        erlc_opts = "//:test_erlc_opts",
    )
