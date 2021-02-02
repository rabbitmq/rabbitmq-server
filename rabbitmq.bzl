load("//bazel_erlang:bazel_erlang_lib.bzl", "bazel_erlang_lib")
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

ERLANG_VERSIONS = [
    "23.1",
    "22.3",
]

_REQUIRED_PLUGINS = [
    "@cuttlefish//:cuttlefish",
    "@ranch//:ranch",
    "@lager//:lager",
    "//deps/rabbit_common:rabbit_common",
    "@ra//:ra",
    "@sysmon-handler//:sysmon_handler",
    "@stdout_formatter//:stdout_formatter",
    "@recon//:recon",
    "@observer_cli//:observer_cli",
    "@osiris//:osiris",
    "//deps/amqp10_common:amqp10_common",
    "//deps/rabbit:rabbit",
    "//deps/rabbit/apps/rabbitmq_prelaunch:rabbitmq_prelaunch",
    "@goldrush//:goldrush",
    "@jsx//:jsx",
    "@credentials-obfuscation//:credentials_obfuscation",
    "@aten//:aten",
    "@gen-batch-server//:gen_batch_server",
]

def required_plugins(erlang_version):
    return [Label("{}@{}".format(p, erlang_version)) for p in _REQUIRED_PLUGINS]

def erlang_libs(**kwargs):
    app_name = kwargs['app_name']
    deps = kwargs.get('deps', [])
    runtime_deps = kwargs.get('runtime_deps', [])
    for erlang_version in ERLANG_VERSIONS:
        kwargs2 = dict(kwargs.items())
        kwargs2.update(
            deps = [dep + "@" + erlang_version for dep in deps],
            runtime_deps = [dep + "@" + erlang_version for dep in runtime_deps],
        )
        bazel_erlang_lib(
            name = "{}@{}".format(app_name, erlang_version),
            erlang_version = erlang_version,
            **kwargs2
        )
        kwargs3 = dict(kwargs2.items())
        erlc_opts = kwargs3.get('erlc_opts', [])
        if "-DTEST" not in erlc_opts:
            erlc_opts = erlc_opts + ["-DTEST"]
        if "+debug_info" not in erlc_opts:
            erlc_opts = erlc_opts + ["+debug_info"]
        kwargs3.update(erlc_opts = erlc_opts)
        bazel_erlang_lib(
            name = "{}_test@{}".format(app_name, erlang_version),
            erlang_version = erlang_version,
            testonly = True,
            **kwargs3
        )

_VERSIONLESS_TOOLS = ["//:fake"]

def ct_tests(**kwargs):
    name = kwargs['name']
    deps = kwargs.get('deps', [])
    tools = kwargs.get('tools', [])
    for erlang_version in ERLANG_VERSIONS:
        kwargs2 = dict(kwargs.items())
        kwargs2.update(
            name = "{}@{}".format(name, erlang_version),
            deps = [dep + "@" + erlang_version for dep in deps],
            tools = [tool + "@" + erlang_version if not tool in _VERSIONLESS_TOOLS else tool for tool in tools],
        )
        ct_test(
            erlang_version = erlang_version,
            tags = ["erlang-{}".format(erlang_version)],
            **kwargs2
        )