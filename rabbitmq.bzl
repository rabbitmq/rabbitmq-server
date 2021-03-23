load("@bazel-erlang//:bazel_erlang_lib.bzl", "erlang_lib", "test_erlang_lib")
load("@bazel-erlang//:ct.bzl", "ct_suite", "ct_test")
load("//deps/rabbitmq_cli:rabbitmqctl.bzl", "rabbitmqctl")
load("//deps/rabbitmq_cli:rabbitmqctl_test.bzl", "rabbitmqctl_test")

RABBITMQ_ERLC_OPTS = []

RABBITMQ_TEST_ERLC_OPTS = [
    "-DTEST",
    "+debug_info",
    "+nowarn_export_all",
]

APP_VERSION = "3.9.0"

def required_plugins(rabbitmq_workspace = "@rabbitmq-server"):
    return [
        # "@cuttlefish//:bazel_erlang_lib",
        # "@ranch//:bazel_erlang_lib",
        # rabbitmq_workspace + "//deps/rabbit_common:bazel_erlang_lib",
        # "@ra//:bazel_erlang_lib",
        # "@sysmon_handler//:bazel_erlang_lib",
        # "@stdout_formatter//:bazel_erlang_lib",
        # "@recon//:bazel_erlang_lib",
        # "@observer_cli//:bazel_erlang_lib",
        # "@osiris//:bazel_erlang_lib",
        # rabbitmq_workspace + "//deps/amqp10_common:bazel_erlang_lib",
        rabbitmq_workspace + "//deps/rabbit:bazel_erlang_lib",
        # rabbitmq_workspace + "//deps/rabbit/apps/rabbitmq_prelaunch:bazel_erlang_lib",
        # "@goldrush//:bazel_erlang_lib",
        # "@jsx//:bazel_erlang_lib",
        # "@credentials_obfuscation//:bazel_erlang_lib",
        # "@aten//:bazel_erlang_lib",
        # "@gen_batch_server//:bazel_erlang_lib",
        # "@syslog//:bazel_erlang_lib",
    ]

def management_plugins(rabbitmq_workspace = "@rabbitmq-server"):
    return [
        rabbitmq_workspace + "//deps/rabbitmq_management:bazel_erlang_lib",
        # rabbitmq_workspace + "//deps/rabbitmq_management_agent:bazel_erlang_lib",
        # rabbitmq_workspace + "//deps/rabbitmq_web_dispatch:bazel_erlang_lib",
        # rabbitmq_workspace + "//deps/amqp_client:bazel_erlang_lib",
        # "@cowboy//:bazel_erlang_lib",
        # "@cowlib//:bazel_erlang_lib",
    ]

def rabbitmq_lib(
        app_name = "",
        app_version = APP_VERSION,
        app_description = "",
        app_module = "",
        app_registered = [],
        app_env = "[]",
        extra_apps = [],
        extra_erlc_opts = [],
        first_srcs = [],
        priv = [],
        build_deps = [],
        deps = [],
        runtime_deps = []):

    erlang_lib(
        app_name = app_name,
        app_version = app_version,
        app_description = app_description,
        app_module = app_module,
        app_registered = app_registered,
        app_env = app_env,
        extra_apps = extra_apps,
        erlc_opts = RABBITMQ_ERLC_OPTS + extra_erlc_opts,
        first_srcs = first_srcs,
        priv = priv,
        build_deps = build_deps,
        deps = deps,
        runtime_deps = runtime_deps,
    )

    test_erlang_lib(
        app_name = app_name,
        app_version = app_version,
        app_description = app_description,
        app_module = app_module,
        app_registered = app_registered,
        app_env = app_env,
        extra_apps = extra_apps,
        erlc_opts = RABBITMQ_ERLC_OPTS + RABBITMQ_TEST_ERLC_OPTS + extra_erlc_opts,
        first_srcs = first_srcs,
        priv = priv,
        build_deps = build_deps,
        deps = deps,
        runtime_deps = runtime_deps,
    )

def rabbitmq_integration_suite(
        data = [],
        test_env = {},
        tools = [],
        deps = [],
        runtime_deps = [],
        **kwargs):
    ct_suite(
        erlc_opts = RABBITMQ_ERLC_OPTS + RABBITMQ_TEST_ERLC_OPTS,
        data = [
            "@rabbitmq_ct_helpers//tools/tls-certs:Makefile",
            "@rabbitmq_ct_helpers//tools/tls-certs:openssl.cnf.in",
        ] + data,
        test_env = dict({
            "RABBITMQ_CT_SKIP_AS_ERROR": "true",
            "RABBITMQ_RUN": "$TEST_SRCDIR/$TEST_WORKSPACE/rabbitmq-for-tests-run",
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/broker-for-tests-home/sbin/rabbitmqctl",
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/broker-for-tests-home/sbin/rabbitmq-plugins",
        }.items() + test_env.items()),
        tools = [
            "//:rabbitmq-for-tests-run",
        ] + tools,
        runtime_deps = [
            "//deps/rabbitmq_cli:elixir_as_bazel_erlang_lib",
            "//deps/rabbitmq_cli:rabbitmqctl",
            # "@credentials_obfuscation//:bazel_erlang_lib",
            # "@goldrush//:bazel_erlang_lib",
            # "@jsx//:bazel_erlang_lib",
            "@rabbitmq_ct_client_helpers//:bazel_erlang_lib",
            # "@recon//:bazel_erlang_lib",
        ] + runtime_deps,
        deps = [
            "//deps/amqp_client:bazel_erlang_lib",
            "@rabbitmq_ct_helpers//:bazel_erlang_lib",
        ] + deps,
        **kwargs
    )
