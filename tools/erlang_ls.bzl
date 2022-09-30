load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
)
load(
    "@rules_erlang//tools:erlang_toolchain.bzl",
    "erlang_dirs",
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
    out = ctx.actions.declare_file(ctx.label.name)

    (erlang_home, _, _) = erlang_dirs(ctx)

    ctx.actions.write(
        output = out,
        content = """otp_path: {erlang_home}
apps_dirs:
  - deps/*
  - deps/rabbit/apps/*
deps_dirs:
  - bazel-bin/external/*
include_dirs:
  - deps
  - deps/*
  - deps/*/include
  - deps/*/src
  - bazel-bin/external
  - bazel-bin/external/*/include
plt_path: bazel-bin/deps/rabbit/.base_plt.plt
""".format(
            erlang_home = erlang_home,
        ),
    )

    return [
        DefaultInfo(files = depset([out])),
    ]

erlang_ls_config = rule(
    implementation = _erlang_ls_config,
    toolchains = [
        "@rules_erlang//tools:toolchain_type",
    ],
)

def _erlang_app_files(ctx, app, directory):
    app_info = app[ErlangAppInfo]
    app_path = path_join(directory, app_info.app_name)
    files = []
    for hdr in app_info.srcs + app_info.beam:
        rp = additional_file_dest_relative_path(app.label, hdr)
        dest = ctx.actions.declare_file(path_join(app_path, rp))
        ctx.actions.symlink(output = dest, target_file = hdr)
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
