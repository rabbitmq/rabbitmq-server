load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
)
load(
    "@rules_erlang//:util.bzl",
    "path_join",
)

def _erlang_ls_config(ctx):
    runtime_prefix = path_join(
        ctx.bin_dir.path,
        ctx.label.package,
        ctx.label.name + ".runfiles",
        ctx.workspace_name,
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = """#!/usr/bin/env bash

set -euo pipefail

BAZEL_OUT_ABSOLUTE_PATH="${{PWD%/{}}}/bazel-out"

cat << EOF
apps_dirs:
- ${{BAZEL_OUT_ABSOLUTE_PATH}}/*/bin/tools/erlang_ls_files/apps/*
deps_dirs:
- ${{BAZEL_OUT_ABSOLUTE_PATH}}/*/bin/tools/erlang_ls_files/deps/*
include_dirs:
- ${{BAZEL_OUT_ABSOLUTE_PATH}}/*/bin/tools/erlang_ls_files/apps
- ${{BAZEL_OUT_ABSOLUTE_PATH}}/*/bin/tools/erlang_ls_files/apps/*/include
- ${{BAZEL_OUT_ABSOLUTE_PATH}}/*/bin/tools/erlang_ls_files/deps
- ${{BAZEL_OUT_ABSOLUTE_PATH}}/*/bin/tools/erlang_ls_files/deps/*/include
EOF
""".format(runtime_prefix),
    )

erlang_ls_config = rule(
    implementation = _erlang_ls_config,
    executable = True,
)

def _deps_symlinks(ctx):
    apps = ctx.attr.apps
    deps = []

    for app in apps:
        app_info = app[ErlangAppInfo]
        for dep in app_info.deps:
            if dep.label.workspace_name != "" and dep not in deps and dep not in apps:
                deps.append(dep)

    files = []
    output = ctx.actions.declare_file(ctx.label.name + ".sh")

    commands = [
        "set -euxo pipefail",
        "",
        "mkdir -p \"{}\"".format(path_join("$BUILD_WORKSPACE_DIRECTORY", ctx.attr.dest)),
        "",
    ]

    for dep in deps:
        app_info = dep[ErlangAppInfo]
        files.extend(app_info.srcs)

        commands.append("ln -s \"{target}\" \"{source}\"".format(
            target = path_join("$PWD", "external", dep.label.workspace_name),
            source = path_join("$BUILD_WORKSPACE_DIRECTORY", ctx.attr.dest, app_info.app_name),
        ))

    ctx.actions.write(
        output = output,
        content = "\n".join(commands),
    )

    return [DefaultInfo(
        runfiles = ctx.runfiles(files = files),
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
