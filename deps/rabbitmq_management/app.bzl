load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":behaviours", ":other_beam"],
    )
    erlang_bytecode(
        name = "behaviours",
        srcs = ["src/rabbit_mgmt_extension.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_management",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = native.glob(
            ["src/**/*.erl"],
            exclude = [
                "src/rabbit_mgmt_extension.erl",
            ],
        ),
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_management",
        beam = [":behaviours"],
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_management_agent:erlang_app",
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
        srcs = ["src/rabbit_mgmt_extension.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_management",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = native.glob(
            ["src/**/*.erl"],
            exclude = [
                "src/rabbit_mgmt_extension.erl",
            ],
        ),
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_management",
        beam = [":test_behaviours"],
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
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
        name = "priv",
        srcs = [
            "priv/www/cli/rabbitmqadmin",
        ] + native.glob(["priv/**/*"]),
    )
    filegroup(
        name = "private_hdrs",
        srcs = native.glob(["src/**/*.hrl"]),
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
        name = "license_files",
        srcs = native.glob(["LICENSE*"]),
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "cache_SUITE_beam_files",
        testonly = True,
        srcs = ["test/cache_SUITE.erl"],
        outs = ["test/cache_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "clustering_SUITE_beam_files",
        testonly = True,
        srcs = ["test/clustering_SUITE.erl"],
        outs = ["test/clustering_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbit_common:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app", "//deps/rabbitmq_management_agent:erlang_app"],
    )
    erlang_bytecode(
        name = "clustering_prop_SUITE_beam_files",
        testonly = True,
        srcs = ["test/clustering_prop_SUITE.erl"],
        outs = ["test/clustering_prop_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbit_common:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app", "//deps/rabbitmq_management_agent:erlang_app", "@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_schema_SUITE.erl"],
        outs = ["test/config_schema_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "listener_config_SUITE_beam_files",
        testonly = True,
        srcs = ["test/listener_config_SUITE.erl"],
        outs = ["test/listener_config_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "rabbit_mgmt_http_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_http_SUITE.erl"],
        outs = ["test/rabbit_mgmt_http_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_mgmt_http_health_checks_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_http_health_checks_SUITE.erl"],
        outs = ["test/rabbit_mgmt_http_health_checks_SUITE.beam"],
        hdrs = ["include/rabbit_mgmt.hrl"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_mgmt_only_http_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_only_http_SUITE.erl"],
        outs = ["test/rabbit_mgmt_only_http_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_mgmt_rabbitmqadmin_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_rabbitmqadmin_SUITE.erl"],
        outs = ["test/rabbit_mgmt_rabbitmqadmin_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "rabbit_mgmt_stats_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_stats_SUITE.erl"],
        outs = ["test/rabbit_mgmt_stats_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbitmq_management_agent:erlang_app", "@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_mgmt_test_db_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_test_db_SUITE.erl"],
        outs = ["test/rabbit_mgmt_test_db_SUITE.beam"],
        hdrs = ["include/rabbit_mgmt.hrl"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app", "//deps/rabbitmq_management_agent:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_mgmt_test_unit_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mgmt_test_unit_SUITE.erl"],
        outs = ["test/rabbit_mgmt_test_unit_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "stats_SUITE_beam_files",
        testonly = True,
        srcs = ["test/stats_SUITE.erl"],
        outs = ["test/stats_SUITE.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbitmq_management_agent:erlang_app", "@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "test_rabbit_mgmt_runtime_parameters_util_beam",
        testonly = True,
        srcs = ["test/rabbit_mgmt_runtime_parameters_util.erl"],
        outs = ["test/rabbit_mgmt_runtime_parameters_util.beam"],
        app_name = "rabbitmq_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
