load("@bazel-erlang//:erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load("@bazel-erlang//:bazel_erlang_lib.bzl", "path_join")
load("@bazel-erlang//:ct.bzl", "sanitize_sname")
load(":rabbitmq_home.bzl", "RabbitmqHomeInfo", "rabbitmq_home_short_path")

def _impl(ctx):
    rabbitmq_home_path = rabbitmq_home_short_path(ctx.attr.home)

    erl_libs = [path_join(rabbitmq_home_path, "plugins")]

    ctx.actions.expand_template(
        template = ctx.file._template,
        output = ctx.outputs.executable,
        substitutions = {
            "{RABBITMQ_HOME}": rabbitmq_home_path,
            "{ERL_LIBS}": ":".join(erl_libs),
            "{ERLANG_HOME}": ctx.attr._erlang_home[ErlangHomeProvider].path,
            "{SNAME}": sanitize_sname("sbb-" + ctx.attr.name),
        },
        is_executable = True,
    )

    runfiles = ctx.runfiles(ctx.attr.home[DefaultInfo].files.to_list())

    return [DefaultInfo(runfiles = runfiles)]

rabbitmq_run = rule(
    implementation = _impl,
    attrs = {
        "_template": attr.label(
            default = Label("//:scripts/bazel/rabbitmq-run.sh"),
            allow_single_file = True,
        ),
        "_erlang_home": attr.label(default = "@bazel-erlang//:erlang_home"),
        "home": attr.label(providers = [RabbitmqHomeInfo]),
    },
    executable = True,
)

def _run_command_impl(ctx):
    ctx.actions.write(
        output = ctx.outputs.executable,
        content = "exec ./{} {} $@".format(
            ctx.attr.rabbitmq_run[DefaultInfo].files_to_run.executable.short_path,
            ctx.attr.subcommand,
        ),
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
