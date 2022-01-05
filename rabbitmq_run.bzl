load("@rules_erlang//:erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load("@rules_erlang//:util.bzl", "path_join", "windows_path")
load("@rules_erlang//:ct.bzl", "sanitize_sname")
load(":rabbitmq_home.bzl", "RabbitmqHomeInfo", "rabbitmq_home_short_path")

def _impl(ctx):
    rabbitmq_home_path = rabbitmq_home_short_path(ctx.attr.home)

    # the rabbitmq-run.sh template only allows a single erl_libs currently
    erl_libs = ctx.configuration.host_path_separator.join([
        path_join(rabbitmq_home_path, "plugins"),
    ])

    sname = sanitize_sname("sbb-" + ctx.attr.name)

    if not ctx.attr.is_windows:
        output = ctx.actions.declare_file(ctx.label.name)
        ctx.actions.expand_template(
            template = ctx.file._template,
            output = output,
            substitutions = {
                "{RABBITMQ_HOME}": rabbitmq_home_path,
                "{ERL_LIBS}": erl_libs,
                "{ERLANG_HOME}": ctx.attr._erlang_home[ErlangHomeProvider].path,
                "{SNAME}": sname,
            },
            is_executable = True,
        )
    else:
        output = ctx.actions.declare_file(ctx.label.name + ".bat")
        ctx.actions.expand_template(
            template = ctx.file._windows_template,
            output = output,
            substitutions = {
                "{RABBITMQ_HOME}": windows_path(rabbitmq_home_path),
                "{ERL_LIBS}": erl_libs,
                "{ERLANG_HOME}": windows_path(ctx.attr._erlang_home[ErlangHomeProvider].path),
                "{SNAME}": sname,
            },
            is_executable = True,
        )

    runfiles = ctx.runfiles(ctx.attr.home[DefaultInfo].files.to_list())

    return [DefaultInfo(
        runfiles = runfiles,
        executable = output,
    )]

rabbitmq_run_private = rule(
    implementation = _impl,
    attrs = {
        "_template": attr.label(
            default = Label("//:scripts/bazel/rabbitmq-run.sh"),
            allow_single_file = True,
        ),
        "_windows_template": attr.label(
            default = Label("//:scripts/bazel/rabbitmq-run.bat"),
            allow_single_file = True,
        ),
        "_erlang_home": attr.label(default = Label("@rules_erlang//:erlang_home")),
        "is_windows": attr.bool(mandatory = True),
        "home": attr.label(providers = [RabbitmqHomeInfo]),
    },
    executable = True,
)

def rabbitmq_run(**kwargs):
    rabbitmq_run_private(
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        **kwargs
    )

def _run_command_impl(ctx):
    if not ctx.attr.is_windows:
        output = ctx.actions.declare_file(ctx.label.name)
        script = "exec ./{} {} $@".format(
            ctx.attr.rabbitmq_run[DefaultInfo].files_to_run.executable.short_path,
            ctx.attr.subcommand,
        )
    else:
        output = ctx.actions.declare_file(ctx.label.name + ".bat")
        script = """@echo off
call {} {} %*
if ERRORLEVEL 1 (
    exit /B %ERRORLEVEL%
)
EXIT /B 0
""".format(
            ctx.attr.rabbitmq_run[DefaultInfo].files_to_run.executable.short_path,
            ctx.attr.subcommand,
        )

    ctx.actions.write(
        output = output,
        content = script,
        is_executable = True,
    )

    return [DefaultInfo(
        runfiles = ctx.attr.rabbitmq_run[DefaultInfo].default_runfiles,
        executable = output,
    )]

rabbitmq_run_command_private = rule(
    implementation = _run_command_impl,
    attrs = {
        "is_windows": attr.bool(mandatory = True),
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

def rabbitmq_run_command(**kwargs):
    rabbitmq_run_command_private(
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        **kwargs
    )
