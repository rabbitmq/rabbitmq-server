load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
)
load(
    "@rules_erlang//:util.bzl",
    "path_join",
)
load(
    "@rules_erlang//private:util.bzl",
    "additional_file_dest_relative_path",
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

def _erlang_app_files(ctx, app, directory):
    app_info = app[ErlangAppInfo]
    app_path = path_join(directory, app_info.app_name)
    files = []
    for f in app_info.srcs + app_info.beam:
        relative_path = additional_file_dest_relative_path(app.label, f)
        dest = ctx.actions.declare_file(path_join(app_path, relative_path))
        ctx.actions.symlink(output = dest, target_file = f)
        files.append(dest)
    return files

def _erlang_ls_tree(ctx):
    apps = ctx.attr.apps
    deps = []

    for app in apps:
        app_info = app[ErlangAppInfo]
        for dep in app_info.deps:
            # this puts non rabbitmq plugins, like amqp10_client into deps,
            # but maybe those should be in apps? Does it matter?
            if dep not in deps and dep not in apps:
                deps.append(dep)

    files = []
    for app in apps:
        files.extend(
            _erlang_app_files(ctx, app, path_join(ctx.label.name, "apps")),
        )
    for dep in deps:
        files.extend(
            _erlang_app_files(ctx, dep, path_join(ctx.label.name, "deps")),
        )

    return [
        DefaultInfo(files = depset(files)),
    ]

erlang_ls_tree = rule(
    implementation = _erlang_ls_tree,
    attrs = {
        "apps": attr.label_list(
            providers = [ErlangAppInfo],
        ),
    },
)
