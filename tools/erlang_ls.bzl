load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
)
load(
    "@rules_erlang//:util.bzl",
    "path_join",
)

def _ln_command(target, source):
    return "ln -nsvf \"{target}\" \"{source}\"".format(
        target = target,
        source = source,
    )

def _deps_symlinks(ctx):
    apps = ctx.attr.apps
    deps = []

    for app in apps:
        app_info = app[ErlangAppInfo]
        for dep in app_info.deps:
            if dep.label.workspace_name != "" and dep not in deps and dep not in apps:
                deps.append(dep)

    output = ctx.actions.declare_file(ctx.label.name + ".sh")

    commands = [
        "set -euo pipefail",
        "",
        "cd $BUILD_WORKSPACE_DIRECTORY",
        "",
        "mkdir -p \"{}\"".format(ctx.attr.dest),
        "",
        "echo Generating symlinks to external deps for erlang_ls+bazel...",
        "",
    ]

    # symlinks for external deps
    for dep in deps:
        app_info = dep[ErlangAppInfo]

        commands.append(_ln_command(
            target = path_join("..", "bazel-$(basename $PWD)", "external", dep.label.workspace_name),
            source = path_join(ctx.attr.dest, app_info.app_name),
        ))

    # special case symlinks for generated sources
    commands.append("")
    commands.append(_ln_command(
        target = path_join("..", "..", "..", "bazel-bin", "deps", "rabbit_common", "include", "rabbit_framing.hrl"),
        source = path_join("deps", "rabbit_common", "include", "rabbit_framing.hrl"),
    ))
    commands.append(_ln_command(
        target = path_join("..", "..", "..", "bazel-bin", "deps", "rabbit_common", "src", "rabbit_framing_amqp_0_8.erl"),
        source = path_join("deps", "rabbit_common", "src", "rabbit_framing_amqp_0_8.erl"),
    ))
    commands.append(_ln_command(
        target = path_join("..", "..", "..", "bazel-bin", "deps", "rabbit_common", "src", "rabbit_framing_amqp_0_9_1.erl"),
        source = path_join("deps", "rabbit_common", "src", "rabbit_framing_amqp_0_9_1.erl"),
    ))

    ctx.actions.write(
        output = output,
        content = "\n".join(commands),
    )

    return [DefaultInfo(
        executable = output,
    )]

deps_symlinks = rule(
    implementation = _deps_symlinks,
    attrs = {
        "apps": attr.label_list(
            providers = [ErlangAppInfo],
        ),
        "dest": attr.string(
            mandatory = True,
        ),
    },
    executable = True,
)
