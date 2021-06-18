load(
    "@bazel-erlang//:bazel_erlang_lib.bzl",
    "DEFAULT_ERLC_OPTS",
    "DEFAULT_TEST_ERLC_OPTS",
    "erlang_lib",
    "test_erlang_lib",
)
load("@bazel-erlang//:ct_sharded.bzl", "ct_suite")
load("//:rabbitmq_home.bzl", "rabbitmq_home")
load("//:rabbitmq_run.bzl", "rabbitmq_run")

RABBITMQ_ERLC_OPTS = DEFAULT_ERLC_OPTS

RABBITMQ_TEST_ERLC_OPTS = DEFAULT_TEST_ERLC_OPTS + [
    "+nowarn_export_all",
]

RABBITMQ_DIALYZER_OPTS = [
    "-Werror_handling",
    "-Wunmatched_returns",
]

APP_VERSION = "3.10.0"

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

def rabbitmq_suite(erlc_opts = [], test_env = {}, **kwargs):
    ct_suite(
        erlc_opts = RABBITMQ_TEST_ERLC_OPTS + erlc_opts,
        test_env = dict({
            "RABBITMQ_CT_SKIP_AS_ERROR": "true",
        }.items() + test_env.items()),
        **kwargs
    )
    return kwargs["name"]

def broker_for_integration_suites():
    rabbitmq_home(
        name = "broker-for-tests-home",
        plugins = [
            "//deps/rabbit:bazel_erlang_lib",
            ":bazel_erlang_lib",
        ],
    )

    rabbitmq_run(
        name = "rabbitmq-for-tests-run",
        home = ":broker-for-tests-home",
    )

def rabbitmq_integration_suite(
        package,
        name = None,
        tags = [],
        data = [],
        erlc_opts = [],
        test_env = {},
        tools = [],
        deps = [],
        runtime_deps = [],
        **kwargs):
    ct_suite(
        name = name,
        suite_name = name,
        tags = tags,
        erlc_opts = RABBITMQ_TEST_ERLC_OPTS + erlc_opts,
        data = [
            "@rabbitmq_ct_helpers//tools/tls-certs:Makefile",
            "@rabbitmq_ct_helpers//tools/tls-certs:openssl.cnf.in",
        ] + data,
        test_env = dict({
            "SKIP_MAKE_TEST_DIST": "true",
            "RABBITMQ_CT_SKIP_AS_ERROR": "true",
            "RABBITMQ_RUN": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/rabbitmq-for-tests-run".format(package),
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmqctl".format(package),
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-plugins".format(package),
            "RABBITMQ_QUEUES": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-queues".format(package),
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
            "//deps/rabbit_common:bazel_erlang_lib",
            "@rabbitmq_ct_helpers//:bazel_erlang_lib",
        ] + deps,
        **kwargs
    )

    ct_suite(
        name = "{}-mixed".format(name),
        suite_name = name,
        tags = tags + ["mixed-version-cluster"],
        erlc_opts = RABBITMQ_TEST_ERLC_OPTS + erlc_opts,
        data = [
            "@rabbitmq_ct_helpers//tools/tls-certs:Makefile",
            "@rabbitmq_ct_helpers//tools/tls-certs:openssl.cnf.in",
        ] + data,
        test_env = dict({
            "SKIP_MAKE_TEST_DIST": "true",
            "RABBITMQ_FEATURE_FLAGS": "",
            "RABBITMQ_RUN": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/rabbitmq-for-tests-run".format(package),
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmqctl".format(package),
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-plugins".format(package),
            "RABBITMQ_QUEUES": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-queues".format(package),
            "RABBITMQ_RUN_SECONDARY": "$TEST_SRCDIR/rabbitmq-server-generic-unix-3.8.17/rabbitmq-run",
        }.items() + test_env.items()),
        tools = [
            ":rabbitmq-for-tests-run",
            "@rabbitmq-server-generic-unix-3.8.17//:rabbitmq-run",
        ] + tools,
        runtime_deps = [
            "//deps/rabbitmq_cli:elixir_as_bazel_erlang_lib",
            "//deps/rabbitmq_cli:rabbitmqctl",
            "@rabbitmq_ct_client_helpers//:bazel_erlang_lib",
        ] + runtime_deps,
        deps = [
            "//deps/amqp_client:bazel_erlang_lib",
            "//deps/rabbit_common:bazel_erlang_lib",
            "@rabbitmq_ct_helpers//:bazel_erlang_lib",
        ] + deps,
        **kwargs
    )

    return kwargs["name"]

def assert_suites(suite_names, suite_files):
    for f in suite_files:
        sn = f.rpartition("/")[-1].replace(".erl", "")
        if not sn in suite_names:
            fail("A bazel rule has not been defined for {} (expected {} in {}".format(f, sn, suite_names))
