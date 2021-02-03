load("//bazel_erlang:erlang_home.bzl", "ErlangHomeProvider")
load("//bazel_erlang:bazel_erlang_lib.bzl", "path_join")
load("//bazel_erlang:ct.bzl", "sanitize_sname")
load(":rabbitmq_home.bzl", "RabbitmqHomeInfo")

# Note: Theses rules take advantage of the fact that when the files from
#       the rabbitmq_home rule are used as runfiles, they are linked in
#       at their declared relative paths. In other words, since
#       rabbitmq_home declares "sbin/rabbitmq-server", is still at
#       "sbin/rabbitmq-server" when our script runs.

def _impl(ctx):
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
            "{RABBITMQ_HOME}": ctx.attr.home.label.name,
            "{ERL_LIBS}": erl_libs,
            "{ERLANG_HOME}": ctx.attr._erlang_home[ErlangHomeProvider].path,
            "{SNAME}": sanitize_sname("sbb-" + ctx.attr.name),
        },
        is_executable = True,
    )

    return [DefaultInfo(
        runfiles = ctx.runfiles(ctx.attr.home[DefaultInfo].files.to_list()),
    )]

rabbitmq_run = rule(
    implementation = _impl,
    attrs = {
        "_template": attr.label(
            default = Label("//:scripts/bazel/rabbitmq-run.sh"),
            allow_single_file = True,
        ),
        "_erlang_home": attr.label(default = "//bazel_erlang:erlang_home"),
        "erlang_version": attr.string(mandatory = True),
        "home": attr.label(providers=[RabbitmqHomeInfo]),
    },
    executable = True,
)

def _run_command_impl(ctx):
    ctx.actions.write(
        output = ctx.outputs.executable,
        content = "exec ./{} {} $@".format(
            ctx.attr.rabbitmq_run[DefaultInfo].files_to_run.executable.short_path,
            ctx.attr.subcommand,
        )
    )

    return [DefaultInfo(
        runfiles = ctx.attr.rabbitmq_run[DefaultInfo].default_runfiles,
    )]

rabbitmq_run_command = rule(
    implementation = _run_command_impl,
    attrs = {
        "rabbitmq_run": attr.label(
            executable = True,
            cfg = "target",
        ),
        "subcommand": attr.string(values = [
            "run-broker",
            "start-background-broker",
            "stop-node",
        ]),
    },
    executable = True,
)