load(
    "@bazel-erlang//:bazel_erlang_lib.bzl",
    "DEFAULT_ERLC_OPTS",
    "DEFAULT_TEST_ERLC_OPTS",
    "erlang_lib",
    "test_erlang_lib",
)
load("@bazel-erlang//:ct.bzl", "ct_suite", "ct_test")
load("//deps/rabbitmq_cli:rabbitmqctl.bzl", "rabbitmqctl")
load("//deps/rabbitmq_cli:rabbitmqctl_test.bzl", "rabbitmqctl_test")

RABBITMQ_ERLC_OPTS = DEFAULT_ERLC_OPTS

RABBITMQ_TEST_ERLC_OPTS = DEFAULT_TEST_ERLC_OPTS + [
    "+nowarn_export_all",
]

APP_VERSION = "3.9.0"

LABELS_WITH_TEST_VERSIONS = [
    "//deps/amqp10_common:bazel_erlang_lib",
    "//deps/rabbit_common:bazel_erlang_lib",
    "//deps/rabbit:bazel_erlang_lib",
    "//deps/rabbit/apps/rabbitmq_prelaunch:bazel_erlang_lib",
]

def with_test_versions(deps):
    r = []
    for d in deps:
        if d in LABELS_WITH_TEST_VERSIONS:
            r.append(d.replace(":bazel_erlang_lib", ":test_bazel_erlang_lib"))
        else:
            r.append(d)
    return r

def rabbitmq_lib(
        app_name = "",
        app_version = APP_VERSION,
        app_description = "",
        app_module = "",
        app_registered = [],
        app_env = "[]",
        extra_apps = [],
        erlc_opts = RABBITMQ_ERLC_OPTS,
        test_erlc_opts = RABBITMQ_TEST_ERLC_OPTS,
        first_srcs = [],
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
        erlc_opts = erlc_opts,
        first_srcs = first_srcs,
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
        erlc_opts = test_erlc_opts,
        first_srcs = first_srcs,
        build_deps = with_test_versions(build_deps),
        deps = with_test_versions(deps),
        runtime_deps = with_test_versions(runtime_deps),
    )

def rabbitmq_integration_suite(
        package,
        data = [],
        extra_erlc_opts = [],
        test_env = {},
        tools = [],
        deps = [],
        runtime_deps = [],
        **kwargs):
    ct_suite(
        erlc_opts = RABBITMQ_TEST_ERLC_OPTS + extra_erlc_opts,
        data = [
            "@rabbitmq_ct_helpers//tools/tls-certs:Makefile",
            "@rabbitmq_ct_helpers//tools/tls-certs:openssl.cnf.in",
        ] + data,
        test_env = dict({
            "RABBITMQ_CT_SKIP_AS_ERROR": "true",
            "RABBITMQ_RUN": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/rabbitmq-for-tests-run".format(package),
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmqctl".format(package),
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-plugins".format(package),
        }.items() + test_env.items()),
        tools = [
            ":rabbitmq-for-tests-run",
        ] + tools,
        runtime_deps = [
            "//deps/rabbitmq_cli:elixir_as_bazel_erlang_lib",
            "//deps/rabbitmq_cli:rabbitmqctl",
            "@rabbitmq_ct_client_helpers//:bazel_erlang_lib",
        ] + runtime_deps,
        deps = [
            "//deps/amqp_client:bazel_erlang_lib",
            "@rabbitmq_ct_helpers//:bazel_erlang_lib",
        ] + deps,
        **kwargs
    )
