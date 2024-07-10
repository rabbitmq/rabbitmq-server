load(
    "@rules_erlang//:erlang_app.bzl",
    "DEFAULT_ERLC_OPTS",
    "DEFAULT_TEST_ERLC_OPTS",
    "erlang_app",
    "test_erlang_app",
)
load(
    "@rules_erlang//:ct.bzl",
    "assert_suites2",
    "ct_test",
)
load("//:rabbitmq_home.bzl", "rabbitmq_home")
load("//:rabbitmq_run.bzl", "rabbitmq_run")

def without(item, elements):
    c = list(elements)
    c.remove(item)
    return c

STARTS_BACKGROUND_BROKER_TAG = "starts-background-broker"

MIXED_VERSION_CLUSTER_TAG = "mixed-version-cluster"

RABBITMQ_ERLC_OPTS = DEFAULT_ERLC_OPTS + [
    "-DINSTR_MOD=gm",
]

RABBITMQ_TEST_ERLC_OPTS = DEFAULT_TEST_ERLC_OPTS + [
    "+nowarn_export_all",
    "-DINSTR_MOD=gm",
]

RABBITMQ_DIALYZER_OPTS = [
    "-Werror_handling",
    "-Wunmatched_returns",
    "-Wunknown",
]

APP_VERSION = "4.0.0"

BROKER_VERSION_REQUIREMENTS_ANY = """
	{broker_version_requirements, []}
"""

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
    "//deps/rabbitmq_stream:erlang_app",
    "//deps/rabbitmq_stream_management:erlang_app",
    "//deps/rabbitmq_top:erlang_app",
    "//deps/rabbitmq_tracing:erlang_app",
    "//deps/rabbitmq_trust_store:erlang_app",
    "//deps/rabbitmq_web_dispatch:erlang_app",
    "//deps/rabbitmq_web_mqtt:erlang_app",
    "//deps/rabbitmq_web_mqtt_examples:erlang_app",
    "//deps/rabbitmq_web_stomp:erlang_app",
    "//deps/rabbitmq_web_stomp_examples:erlang_app",
]

LABELS_WITH_TEST_VERSIONS = [
    "//deps/amqp10_common:erlang_app",
    "//deps/rabbit_common:erlang_app",
    "//deps/rabbitmq_prelaunch:erlang_app",
    "//deps/rabbit:erlang_app",
]

def all_plugins(rabbitmq_workspace = "@rabbitmq-server"):
    return [
        Label("{}{}".format(rabbitmq_workspace, p))
        for p in ALL_PLUGINS
    ]

def with_test_versions(deps):
    r = []
    for d in deps:
        if d in LABELS_WITH_TEST_VERSIONS:
            r.append(d.replace(":erlang_app", ":test_erlang_app"))
        else:
            r.append(d)
    return r

def rabbitmq_app(
        name = "erlang_app",
        app_name = "",
        app_version = APP_VERSION,
        app_description = "",
        app_module = "",
        app_registered = [],
        app_env = "",
        app_extra_keys = "",
        extra_apps = [],
        beam_files = [":beam_files"],
        hdrs = None,
        srcs = [":all_srcs"],
        priv = [":priv"],
        license_files = [":license_files"],
        deps = [],
        testonly = False):
    if name != "erlang_app":
        fail("name attr exists for compatibility only, and must be set to '\"erlang_app\"'")
    if beam_files != [":beam_files"]:
        fail("beam_files attr exists for compatibility only, and must be set to '[\":beam_files\"]'")
    if hdrs != [":public_hdrs"]:
        fail("hdrs attr exists for compatibility only, and must be set to '[\":public_hdrs\"]'")

    erlang_app(
        name = "erlang_app",
        app_name = app_name,
        app_version = app_version,
        app_description = app_description,
        app_module = app_module,
        app_registered = app_registered,
        app_env = app_env,
        app_extra_keys = app_extra_keys,
        extra_apps = extra_apps,
        beam_files = beam_files,
        hdrs = [":public_hdrs"],
        srcs = srcs,
        priv = priv,
        license_files = license_files,
        deps = deps,
        testonly = testonly,
    )

    test_erlang_app(
        name = "test_erlang_app",
        app_name = app_name,
        app_version = app_version,
        app_description = app_description,
        app_module = app_module,
        app_registered = app_registered,
        app_env = app_env,
        app_extra_keys = app_extra_keys,
        extra_apps = extra_apps,
        beam_files = [":test_beam_files"],
        hdrs = [":public_and_private_hdrs"],
        srcs = srcs,
        priv = priv,
        license_files = license_files,
        deps = with_test_versions(deps),
    )

def rabbitmq_suite(
        name = None,
        suite_name = None,
        data = [],
        additional_beam = [],
        test_env = {},
        deps = [],
        runtime_deps = [],
        **kwargs):
    app_name = native.package_name().rpartition("/")[-1]
    # suite_name exists in the underying ct_test macro, but we don't
    # want to use the arg in rabbitmq-server, for the sake of clarity
    if suite_name != None:
        fail("rabbitmq_suite cannot be called with a suite_name attr")
    ct_test(
        name = name,
        app_name = app_name,
        compiled_suites = [":{}_beam_files".format(name)] + additional_beam,
        data = native.glob(["test/{}_data/**/*".format(name)]) + data,
        test_env = dict({
            "RABBITMQ_CT_SKIP_AS_ERROR": "true",
            "LANG": "C.UTF-8",
            "COVERDATA_TO_LCOV_APPS_DIRS": "deps:deps/rabbit/apps",
        }.items() + test_env.items()),
        deps = [":test_erlang_app"] + deps + runtime_deps,
        **kwargs
    )
    return name

def broker_for_integration_suites(extra_plugins = []):
    rabbitmq_home(
        name = "broker-for-tests-home",
        plugins = [
            "//deps/rabbit:test_erlang_app",
            ":test_erlang_app",
        ] + extra_plugins,
        testonly = True,
    )

    rabbitmq_run(
        name = "rabbitmq-for-tests-run",
        home = ":broker-for-tests-home",
        testonly = True,
    )

def rabbitmq_integration_suite(
        name = None,
        suite_name = None,
        tags = [],
        data = [],
        erlc_opts = [],
        additional_beam = [],
        test_env = {},
        tools = [],
        deps = [],
        runtime_deps = [],
        **kwargs):
    app_name = native.package_name().rpartition("/")[-1]
    # suite_name exists in the underying ct_test macro, but we don't
    # want to use the arg in rabbitmq-server, for the sake of clarity
    if suite_name != None:
        fail("rabbitmq_integration_suite cannot be called with a suite_name attr")
    assumed_deps = [
        ":test_erlang_app",
        "//deps/rabbit_common:erlang_app",
        "//deps/rabbitmq_ct_helpers:erlang_app",
        "@rules_elixir//elixir",
        "//deps/rabbitmq_cli:erlang_app",
        "//deps/rabbitmq_ct_client_helpers:erlang_app",
    ]
    package = native.package_name()
    if package != "deps/amqp_client":
        assumed_deps.append("//deps/amqp_client:erlang_app")

    ct_test(
        name = name,
        app_name = app_name,
        suite_name = name,
        compiled_suites = [":{}_beam_files".format(name)] + additional_beam,
        tags = tags + [STARTS_BACKGROUND_BROKER_TAG],
        data = native.glob(["test/{}_data/**/*".format(name)]) + data,
        test_env = dict({
            "SKIP_MAKE_TEST_DIST": "true",
            "RABBITMQ_CT_SKIP_AS_ERROR": "true",
            "RABBITMQ_RUN": "$(location :rabbitmq-for-tests-run)",
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmqctl".format(package),
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-plugins".format(package),
            "RABBITMQ_QUEUES": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-queues".format(package),
            "LANG": "C.UTF-8",
            "COVERDATA_TO_LCOV_APPS_DIRS": "deps:deps/rabbit/apps",
        }.items() + test_env.items()),
        tools = [
            ":rabbitmq-for-tests-run",
        ] + tools,
        deps = assumed_deps + deps + runtime_deps,
        **kwargs
    )

    ct_test(
        name = name + "-mixed",
        suite_name = name,
        compiled_suites = [":{}_beam_files".format(name)] + additional_beam,
        tags = tags + [STARTS_BACKGROUND_BROKER_TAG, MIXED_VERSION_CLUSTER_TAG],
        data = native.glob(["test/{}_data/**/*".format(name)]) + data,
        test_env = dict({
            "SKIP_MAKE_TEST_DIST": "true",
            # The feature flags listed below are required. This means they must be enabled in mixed-version testing
            # before even starting the cluster because newer nodes don't have the corresponding compatibility/migration code.
            "RABBITMQ_FEATURE_FLAGS":
            # required starting from 3.11.0 in rabbit:
            "quorum_queue,implicit_default_bindings,virtual_host_metadata,maintenance_mode_status,user_limits," +
            # required starting from 3.12.0 in rabbit:
            "feature_flags_v2,stream_queue,classic_queue_type_delivery_support,classic_mirrored_queue_version," +
            "stream_single_active_consumer,direct_exchange_routing_v2,listener_records_in_ets,tracking_records_in_ets," +
            # required starting from 3.12.0 in rabbitmq_management_agent:
            # empty_basic_get_metric, drop_unroutable_metric
            # required starting from 4.0 in rabbit:
            "message_containers,stream_update_config_command,stream_filtering,stream_sac_coordinator_unblock_group,restart_streams",
            "RABBITMQ_RUN": "$(location :rabbitmq-for-tests-run)",
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmqctl".format(package),
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-plugins".format(package),
            "RABBITMQ_QUEUES": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-queues".format(package),
            "RABBITMQ_RUN_SECONDARY": "$(location @rabbitmq-server-generic-unix-3.13//:rabbitmq-run)",
            "LANG": "C.UTF-8",
        }.items() + test_env.items()),
        tools = [
            ":rabbitmq-for-tests-run",
            "@rabbitmq-server-generic-unix-3.13//:rabbitmq-run",
        ] + tools,
        deps = assumed_deps + deps + runtime_deps,
        **kwargs
    )

    return name

def assert_suites(**kwargs):
    assert_suites2(**kwargs)
