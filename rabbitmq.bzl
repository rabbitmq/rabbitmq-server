load("//bazel_erlang:bazel_erlang_lib.bzl", "app_file", "bazel_erlang_lib", "erlc", "erlang_lib")
load("//bazel_erlang:ct.bzl", "ct_test")
load("//deps/rabbitmq_cli:rabbitmqctl.bzl", "rabbitmqctl")
load("//deps/rabbitmq_cli:rabbitmqctl_test.bzl", "rabbitmqctl_test")

_LAGER_EXTRA_SINKS = [
    "rabbit_log",
    "rabbit_log_channel",
    "rabbit_log_connection",
    "rabbit_log_feature_flags",
    "rabbit_log_federation",
    "rabbit_log_ldap",
    "rabbit_log_mirroring",
    "rabbit_log_osiris",
    "rabbit_log_prelaunch",
    "rabbit_log_queue",
    "rabbit_log_ra",
    "rabbit_log_shovel",
    "rabbit_log_upgrade",
]

RABBITMQ_ERLC_OPTS = [
    "+{parse_transform,lager_transform}",
    "+{lager_extra_sinks,[" + ",".join(_LAGER_EXTRA_SINKS) + "]}",
]

APP_VERSION = "3.9.0"

REQUIRED_PLUGINS = [
    "@cuttlefish//:bazel_erlang_lib",
    "@ranch//:bazel_erlang_lib",
    "@lager//:bazel_erlang_lib",
    "//deps/rabbit_common:bazel_erlang_lib",
    "@ra//:bazel_erlang_lib",
    "@sysmon-handler//:bazel_erlang_lib",
    "@stdout_formatter//:bazel_erlang_lib",
    "@recon//:bazel_erlang_lib",
    "@observer_cli//:bazel_erlang_lib",
    "@osiris//:bazel_erlang_lib",
    "//deps/amqp10_common:bazel_erlang_lib",
    "//deps/rabbit:bazel_erlang_lib",
    "//deps/rabbit/apps/rabbitmq_prelaunch:bazel_erlang_lib",
    "@goldrush//:bazel_erlang_lib",
    "@jsx//:bazel_erlang_lib",
    "@credentials-obfuscation//:bazel_erlang_lib",
    "@aten//:bazel_erlang_lib",
    "@gen-batch-server//:bazel_erlang_lib",
]

def rabbitmq_lib(
    app_name="",
    app_version=APP_VERSION,
    app_description="",
    app_module="",
    app_registered=[],
    app_env="[]",
    extra_apps=[],
    extra_erlc_opts=[],
    first_srcs=[],
    priv=[],
    deps=[],
    runtime_deps=[]):

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
        deps = deps,
        runtime_deps = runtime_deps,
    )

    test_erlc_opts = RABBITMQ_ERLC_OPTS + extra_erlc_opts + [
        "-DTEST",
        "+debug_info",
    ]

    all_test_beam = []

    if len(first_srcs) > 0:
        all_test_beam = [":first_test_beam_files"]
        erlc(
            name = "first_test_beam_files",
            hdrs = native.glob(["include/*.hrl", "src/*.hrl"]),
            srcs = native.glob(first_srcs),
            erlc_opts = test_erlc_opts,
            dest = "src",
            deps = deps,
        )

    erlc(
        name = "test_beam_files",
        hdrs = native.glob(["include/*.hrl", "src/*.hrl"]),
        srcs = native.glob(["src/*.erl"], exclude=first_srcs),
        beam = all_test_beam,
        erlc_opts = test_erlc_opts,
        dest = "src",
        deps = deps,
    )

    all_test_beam = all_test_beam + [":test_beam_files"]

    bazel_erlang_lib(
        name = "test_bazel_erlang_lib",
        app_name = app_name,
        app_version = APP_VERSION,
        hdrs = native.glob(["include/*.hrl"]),
        app = ":app_file",
        beam = all_test_beam,
    )
