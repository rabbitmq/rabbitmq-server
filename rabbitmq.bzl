load(
    "@rules_erlang//:erlang_app.bzl",
    "DEFAULT_ERLC_OPTS",
    "DEFAULT_TEST_ERLC_OPTS",
    "erlang_app",
    "test_erlang_app",
)
load("@rules_erlang//compat:erlang_mk.bzl", "app_src_from_erlang_mk_makefile")
load("@rules_erlang//:erlang_app_sources.bzl", "erlang_app_sources")
load("@rules_erlang//:extract_app.bzl", "extract_app")
load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
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

ALL_PLUGIN_NAMES = [
    "rabbit",
    "rabbitmq_amqp1_0",
    "rabbitmq_auth_backend_cache",
    "rabbitmq_auth_backend_http",
    "rabbitmq_auth_backend_ldap",
    "rabbitmq_auth_backend_oauth2",
    "rabbitmq_auth_mechanism_ssl",
    "rabbitmq_consistent_hash_exchange",
    "rabbitmq_event_exchange",
    "rabbitmq_federation",
    "rabbitmq_federation_management",
    "rabbitmq_jms_topic_exchange",
    "rabbitmq_management",
    "rabbitmq_mqtt",
    "rabbitmq_peer_discovery_aws",
    "rabbitmq_peer_discovery_consul",
    "rabbitmq_peer_discovery_etcd",
    "rabbitmq_peer_discovery_k8s",
    "rabbitmq_prometheus",
    "rabbitmq_random_exchange",
    "rabbitmq_recent_history_exchange",
    "rabbitmq_sharding",
    "rabbitmq_shovel",
    "rabbitmq_shovel_management",
    "rabbitmq_stomp",
    "rabbitmq_stream",
    "rabbitmq_stream_management",
    "rabbitmq_top",
    "rabbitmq_tracing",
    "rabbitmq_trust_store",
    "rabbitmq_web_dispatch",
    "rabbitmq_web_mqtt",
    "rabbitmq_web_mqtt_examples",
    "rabbitmq_web_stomp",
    "rabbitmq_web_stomp_examples",
]

ALL_PLUGINS = [
    "//deps/{}:erlang_app".format(p)
    for p in ALL_PLUGIN_NAMES
]

LABELS_WITH_TEST_VERSIONS = [
    "//deps/amqp10_common",
    "//deps/rabbit_common",
    "//deps/rabbitmq_prelaunch",
    "//deps/rabbit",
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
            r.append(d + ":test_erlang_app")
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

def rabbitmq_app2(
        name = "erlang_app",
        app_name = None,
        extract_from = "apps",
        app_src = None,
        extra_apps = [],
        public_hdrs = None,
        srcs = None,
        priv = None,
        deps = [],
        testonly = False):
    if name != "erlang_app":
        fail("name attr exists for compatibility only, and must be set to '\"erlang_app\"'")

    if app_src == None:
        app_src_from_erlang_mk_makefile(
            name = "app_src",
            srcs = native.glob(
                [
                    "**/*",
                ],
                exclude = ["BUILD.bazel"],
            ),
            make_vars = {
                "PROJECT_VERSION": APP_VERSION,
            },
            out = "src/%s.app.src" % app_name,
            testonly = testonly,
        )
        app_src = ":app_src"

    if not testonly:
        erlang_app_sources(
            name = "srcs",
            srcs = srcs,
            app_name = app_name,
            app_src = app_src,
            erlc_opts_file = "//:erlc_opts_file",
            public_hdrs = public_hdrs,
            priv = priv,
            visibility = ["//visibility:public"],
        )

    erlang_app_sources(
        name = "test_srcs",
        srcs = srcs,
        app_name = app_name,
        app_src = app_src,
        erlc_opts_file = "//:test_erlc_opts_file",
        public_hdrs = public_hdrs,
        priv = priv,
        visibility = ["//visibility:public"],
        testonly = True,
    )

    if not testonly:
        extract_app(
            name = "erlang_app",
            app_name = app_name,
            erl_libs = "//:%s" % extract_from,
            extra_apps = extra_apps,
            deps = deps,
            visibility = ["//visibility:public"],
        )

    extract_app(
        name = "test_erlang_app",
        testonly = True,
        verify = False,
        app_name = app_name,
        beam_dest = "test",
        erl_libs = "//:test_%s" % extract_from,
        extra_apps = extra_apps,
        deps = with_test_versions(deps),
        visibility = ["//visibility:public"],
    )

    if not testonly:
        native.alias(
            name = app_name,
            actual = ":erlang_app",
            visibility = ["//visibility:public"],
        )
    else:
        native.alias(
            name = app_name,
            actual = ":test_erlang_app",
            visibility = ["//visibility:public"],
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

def rabbitmq_suite2(
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

    package = native.package_name()

    assumed_deps = [":test_erlang_app"]
    if package != "deps/amqp_client":
        assumed_deps.append("//deps/amqp_client")
    if package != "deps/rabbitmq_ct_helpers":
        assumed_deps.append("//deps/rabbitmq_ct_helpers")

    erlang_bytecode(
        name = "{}_beam_files".format(name),
        hdrs = native.glob([
            "include/*.hrl",
            "src/*.hrl",
            "test/*.hrl",
        ]),
        srcs = [
            "test/{}.erl".format(name),
        ],
        deps = assumed_deps + deps,
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        testonly = True,
    )

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
        deps = (assumed_deps + deps + runtime_deps),
        **kwargs
    )
    return name

def test_helpers(
        name = "test_helpers",
        deps = [":test_erlang_app"],
        **kwargs):
    erlang_bytecode(
        name = name,
        testonly = True,
        srcs = native.glob(
            ["test/**/*.erl"],
            exclude = ["test/*_SUITE.erl"],
        ),
        hdrs = native.glob([
            "include/*.hrl",
            "src/*.hrl",
        ]),
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = deps,
        **kwargs
    )

def broker_for_integration_suites(extra_plugins = []):
    apps = ["rabbit"]
    if native.package_name() != "deps/rabbit":
        (_, _, app) = native.package_name().rpartition("/")
        apps.append(app)

    rabbitmq_home(
        name = "broker-for-tests-home",
        erl_libs = [
            "@erlang_packages//:deps",
            "@erlang_packages//:test_deps",
            "//:test_early_apps",
            "//:rabbitmq_cli",
            "//:test_apps",
            "//:bazel_native_deps",
        ],
        apps = apps + extra_plugins,
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
        "//deps/rabbitmq_cli:elixir",
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
            "stream_single_active_consumer,direct_exchange_routing_v2,listener_records_in_ets,tracking_records_in_ets",
            # required starting from 3.12.0 in rabbitmq_management_agent:
            # empty_basic_get_metric, drop_unroutable_metric
            "RABBITMQ_RUN": "$(location :rabbitmq-for-tests-run)",
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmqctl".format(package),
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-plugins".format(package),
            "RABBITMQ_QUEUES": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-queues".format(package),
            "RABBITMQ_RUN_SECONDARY": "$(location @rabbitmq-server-generic-unix-3.12//:rabbitmq-run)",
            "LANG": "C.UTF-8",
        }.items() + test_env.items()),
        tools = [
            ":rabbitmq-for-tests-run",
            "@rabbitmq-server-generic-unix-3.12//:rabbitmq-run",
        ] + tools,
        deps = assumed_deps + deps + runtime_deps,
        **kwargs
    )

    return name

def _unique(items):
    r = []
    for item in items:
        if item not in r:
            r.append(item)
    return r

def rabbitmq_integration_suite2(
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

    package = native.package_name()

    assumed_deps = [":test_erlang_app"]
    if package != "deps/amqp_client":
        assumed_deps.append("//deps/amqp_client")
    if package != "deps/rabbitmq_ct_helpers":
        assumed_deps.append("//deps/rabbitmq_ct_helpers")

    erlang_bytecode(
        name = "{}_beam_files".format(name),
        hdrs = native.glob([
            "include/*.hrl",
            "src/*.hrl",
            "test/*.hrl",
        ]),
        srcs = [
            "test/{}.erl".format(name),
        ],
        deps = assumed_deps + deps,
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        testonly = True,
    )

    assumed_runtime_deps = [
        "//deps/rabbitmq_cli:elixir",
        "//deps/rabbitmq_cli:erlang_app",
    ]
    if package != "deps/rabbit_common":
        assumed_runtime_deps.append("//deps/rabbit_common")
    if package != "deps/rabbitmq_ct_client_helpers":
        assumed_runtime_deps.append("//deps/rabbitmq_ct_client_helpers")

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
        deps = assumed_deps + deps + assumed_runtime_deps + runtime_deps,
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
            "stream_single_active_consumer,direct_exchange_routing_v2,listener_records_in_ets,tracking_records_in_ets",
            # required starting from 3.12.0 in rabbitmq_management_agent:
            # empty_basic_get_metric, drop_unroutable_metric
            "RABBITMQ_RUN": "$(location :rabbitmq-for-tests-run)",
            "RABBITMQCTL": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmqctl".format(package),
            "RABBITMQ_PLUGINS": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-plugins".format(package),
            "RABBITMQ_QUEUES": "$TEST_SRCDIR/$TEST_WORKSPACE/{}/broker-for-tests-home/sbin/rabbitmq-queues".format(package),
            "RABBITMQ_RUN_SECONDARY": "$(location @rabbitmq-server-generic-unix-3.12//:rabbitmq-run)",
            "LANG": "C.UTF-8",
        }.items() + test_env.items()),
        tools = [
            ":rabbitmq-for-tests-run",
            "@rabbitmq-server-generic-unix-3.12//:rabbitmq-run",
        ] + tools,
        deps = assumed_deps + deps + assumed_runtime_deps + runtime_deps,
        **kwargs
    )

    return name

def assert_suites(**kwargs):
    assert_suites2(**kwargs)
