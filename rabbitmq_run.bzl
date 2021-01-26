load("//bazel_erlang:erlang_home.bzl", "ErlangHomeProvider")
load("//bazel_erlang:bazel_erlang_lib.bzl", "path_join")
load("//bazel_erlang:ct.bzl", "sanitize_sname")
load(":rabbitmq_home.bzl", "RabbitmqHomeInfo")

# Note: Theses rules take advantage of the fact that when the files from
#       the rabbitmq_home rule are used as runfiles, they are linked in
#       at their declared relative paths. In other words, since
#       rabbitmq_home declares "sbin/rabbitmq-server", is still at
#       "sbin/rabbitmq-server" when our script runs.

def _run_broker_impl(ctx):
    rabbitmq_home = ctx.attr.home[RabbitmqHomeInfo]

    if rabbitmq_home.erlang_version != ctx.attr.erlang_version:
        fail("Mismatched erlang versions", ctx.attr.erlang_version, rabbitmq_home.erlang_version)

    ctx.actions.expand_template(
        template = ctx.file._template,
        output = ctx.outputs.executable,
        substitutions = {
            "{PATH_PREFIX}": ctx.attr.home.label.name,
        },
        is_executable = True,
    )

    return [DefaultInfo(
        runfiles = ctx.runfiles(ctx.attr.home[DefaultInfo].files.to_list()),
    )]

run_broker = rule(
    implementation = _run_broker_impl,
    attrs = {
        "_template": attr.label(
            default = Label("//:scripts/bazel/run_broker.sh"),
            allow_single_file = True,
        ),
        "erlang_version": attr.string(mandatory = True),
        "home": attr.label(providers=[RabbitmqHomeInfo]),
    },
    executable = True,
)

def _start_background_broker_impl(ctx):
    rabbitmq_home = ctx.attr.home[RabbitmqHomeInfo]

    if rabbitmq_home.erlang_version != ctx.attr.erlang_version:
        fail("Mismatched erlang versions", ctx.attr.erlang_version, rabbitmq_home.erlang_version)

    erl_libs = ":".join(
        [p.short_path for p in rabbitmq_home.plugins]
    )

    ctx.actions.expand_template(
        template = ctx.file._template,
        output = ctx.outputs.executable,
        substitutions = {
            "{PATH_PREFIX}": ctx.attr.home.label.name,
            "{ERLANG_HOME}": ctx.attr._erlang_home[ErlangHomeProvider].path,
            "{SNAME}": sanitize_sname("sbb-" + ctx.attr.name),
        },
        is_executable = True,
    )

    return [DefaultInfo(
        runfiles = ctx.runfiles(ctx.attr.home[DefaultInfo].files.to_list()),
    )]


start_background_broker = rule(
    implementation = _start_background_broker_impl,
    attrs = {
        "_template": attr.label(
            default = Label("//:scripts/bazel/start_background_broker.sh"),
            allow_single_file = True,
        ),
        "_erlang_home": attr.label(default = "//bazel_erlang:erlang_home"),
        "erlang_version": attr.string(mandatory = True),
        "home": attr.label(providers=[RabbitmqHomeInfo]),
    },
    executable = True,
)