load(
    "@rules_erlang//:erlang_app.bzl",
    "DEFAULT_ERLC_OPTS",
    "DEFAULT_TEST_ERLC_OPTS",
    "erlang_app",
    "test_erlang_app",
)
load("@rules_erlang//:ct_sharded.bzl", "ct_suite", "ct_suite_variant")
load("//:rabbitmq_home.bzl", "rabbitmq_home")
load("//:rabbitmq_run.bzl", "rabbitmq_run")

def without(item, elements):
    c = list(elements)
    c.remove(item)
    return c

_LAGER_EXTRA_SINKS = [
    "rabbit_log",
    "rabbit_log_channel",
    "rabbit_log_connection",
    "rabbit_log_feature_flags",
    "rabbit_log_federation",
    "rabbit_log_ldap",
    "rabbit_log_mirroring",
    "rabbit_log_prelaunch",
    "rabbit_log_queue",
    "rabbit_log_ra",
    "rabbit_log_shovel",
    "rabbit_log_upgrade",
]

STARTS_BACKGROUND_BROKER_TAG = "starts-background-broker"

MIXED_VERSION_CLUSTER_TAG = "mixed-version-cluster"

RABBITMQ_ERLC_OPTS = DEFAULT_ERLC_OPTS + [
    "+{parse_transform,lager_transform}",
    "+{lager_extra_sinks,[" + ",".join(_LAGER_EXTRA_SINKS) + "]}",
    "-DINSTR_MOD=gm",
]
RABBITMQ_ERLC_OPTS.remove("+deterministic")

RABBITMQ_TEST_ERLC_OPTS = DEFAULT_TEST_ERLC_OPTS + [
    "+{parse_transform,lager_transform}",
    "+{lager_extra_sinks,[" + ",".join(_LAGER_EXTRA_SINKS) + "]}",
    "+nowarn_export_all",
    "-DINSTR_MOD=gm",
]
RABBITMQ_TEST_ERLC_OPTS.remove("+deterministic")

RABBITMQ_DIALYZER_OPTS = [
    "-Werror_handling",
    "-Wunmatched_returns",
]

APP_VERSION = "3.8"

ALL_PLUGINS = [
    "//deps/rabbit:erlang_app",
    "//deps/rabbitmq_amqp1_0:erlang_app",
    "//deps/rabbitmq_auth_backend_cache:erlang_app",
    "//deps/rabbitmq_auth_backend_http:erlang_app",
    "//deps/rabbitmq_auth_backend_ldap:erlang_app",
    "//deps/rabbitmq_auth_backend_oauth2:erlang_app",
    "//deps/rabbitmq_auth_mechanism_ssl:erlang_app",
    "//deps/rabbitmq_consistent_hash_exchange:erlang_app",
    "//deps/rabbitmq_event_exchange:erlang_app",
    "//deps/rabbitmq_federation:erlang_app",
    "//deps/rabbitmq_federation_management:erlang_app",
    "//deps/rabbitmq_jms_topic_exchange:erlang_app",
    "//deps/rabbitmq_management:erlang_app",
    "//deps/rabbitmq_mqtt:erlang_app",
    "//deps/rabbitmq_peer_discovery_aws:erlang_app",
    "//deps/rabbitmq_peer_discovery_consul:erlang_app",
    "//deps/rabbitmq_peer_discovery_etcd:erlang_app",
    "//deps/rabbitmq_peer_discovery_k8s:erlang_app",
    "//deps/rabbitmq_prometheus:erlang_app",
    "//deps/rabbitmq_random_exchange:erlang_app",
    "//deps/rabbitmq_recent_history_exchange:erlang_app",
    "//deps/rabbitmq_sharding:erlang_app",
    "//deps/rabbitmq_shovel:erlang_app",
    "//deps/rabbitmq_shovel_management:erlang_app",
    "//deps/rabbitmq_stomp:erlang_app",
    "//deps/rabbitmq_top:erlang_app",
    "//deps/rabbitmq_tracing:erlang_app",
    "//deps/rabbitmq_trust_store:erlang_app",
    "//deps/rabbitmq_web_dispatch:erlang_app",
    "//deps/rabbitmq_web_mqtt:erlang_app",
    "//deps/rabbitmq_web_stomp:erlang_app",
]

LABELS_WITH_TEST_VERSIONS = [
    "//deps/amqp10_common:erlang_app",
    "//deps/rabbit_common:erlang_app",
    "//deps/rabbit:erlang_app",
    "//deps/rabbit/apps/rabbitmq_prelaunch:erlang_app",
]

def all_plugins(rabbitmq_workspace = "@rabbitmq-server"):
    return [rabbitmq_workspace + p for p in ALL_PLUGINS]

def with_test_versions(deps):
    r = []
    for d in deps:
        if d in LABELS_WITH_TEST_VERSIONS:
            r.append(d.replace(":erlang_app", ":test_erlang_app"))
        else:
            r.append(d)
    return r

def rabbitmq_app(
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
        extra_priv = [],
        build_deps = [],
        deps = [],
        runtime_deps = []):
    erlang_app(
        app_name = app_name,
        app_version = app_version,
        app_description = app_description,
        app_module = app_module,
        app_registered = app_registered,
        app_env = app_env,
        extra_apps = extra_apps,
        extra_priv = extra_priv,
        erlc_opts = erlc_opts,
        first_srcs = first_srcs,
        build_deps = build_deps,
        deps = deps,
        runtime_deps = runtime_deps,
    )

    test_erlang_app(
        app_name = app_name,
        app_version = app_version,
        app_description = app_description,
        app_module = app_module,
        app_registered = app_registered,
        app_env = app_env,
        extra_apps = extra_apps,
        extra_priv = extra_priv,
        erlc_opts = test_erlc_opts,
        first_srcs = first_srcs,
        build_deps = with_test_versions(build_deps),
        deps = with_test_versions(deps),
        runtime_deps = with_test_versions(runtime_deps),
    )

def rabbitmq_suite(deps = [], erlc_opts = [], test_env = {}, **kwargs):
    ct_suite(
        deps = ["@lager//:erlang_app"] + deps,
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
            "//deps/rabbit:erlang_app",
            ":erlang_app",
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
        additional_hdrs = [],
        additional_srcs = [],
        test_env = {},
        tools = [],
        deps = [],
        runtime_deps = [],
        **kwargs):
    ct_suite(
        name = name,
        suite_name = name,
        tags = tags + [STARTS_BACKGROUND_BROKER_TAG],
        erlc_opts = RABBITMQ_TEST_ERLC_OPTS + erlc_opts,
        additional_hdrs = additional_hdrs,
        additional_srcs = additional_srcs,
        data = data,
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
            "//deps/rabbitmq_cli:elixir_app",
            "//deps/rabbitmq_cli:rabbitmqctl",
            "//deps/rabbitmq_ct_client_helpers:erlang_app",
        ] + runtime_deps,
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "@lager//:erlang_app",
            "//deps/rabbitmq_ct_helpers:erlang_app",
        ] + deps,
        **kwargs
    )

    ct_suite_variant(
        name = name + "-mixed",
        suite_name = name,
        tags = tags + [STARTS_BACKGROUND_BROKER_TAG, MIXED_VERSION_CLUSTER_TAG],
        data = data,
        test_env = dict({
            "SKIP_MAKE_TEST_DIST": "true",
            "RABBITMQ_FEATURE_FLAGS": "",
            "RABBITMQ_RUN": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/rabbitmq-for-tests-run".format(package),
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmqctl".format(package),
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-plugins".format(package),
            "RABBITMQ_QUEUES": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-queues".format(package),
            "RABBITMQ_RUN_SECONDARY": "$TEST_SRCDIR/rabbitmq-server-generic-unix-3.7.28/rabbitmq-run",
        }.items() + test_env.items()),
        tools = [
            ":rabbitmq-for-tests-run",
            "@rabbitmq-server-generic-unix-3.7.28//:rabbitmq-run",
        ] + tools,
        runtime_deps = [
            "//deps/rabbitmq_cli:elixir_app",
            "//deps/rabbitmq_cli:rabbitmqctl",
            "//deps/rabbitmq_ct_client_helpers:erlang_app",
        ] + runtime_deps,
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_ct_helpers:erlang_app",
        ] + deps,
        **kwargs
    )

    return name

def assert_suites(suite_names, suite_files):
    for f in suite_files:
        sn = f.rpartition("/")[-1].replace(".erl", "")
        if not sn in suite_names:
            fail("A bazel rule has not been defined for {} (expected {} in {}".format(f, sn, suite_names))
