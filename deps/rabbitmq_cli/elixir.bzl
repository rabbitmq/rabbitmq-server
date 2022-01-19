load("@rules_erlang//:erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load("@rules_erlang//:erlang_app_info.bzl", "ErlangAppInfo")
load("@rules_erlang//:util.bzl", "path_join", "windows_path")
load("//:elixir_home.bzl", "ElixirHomeProvider")

def _impl(ctx):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version
    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path
    elixir_home = ctx.attr._elixir_home[ElixirHomeProvider].path

    ebin = ctx.actions.declare_directory(path_join(ctx.attr.name, "ebin"))

    if not ctx.attr.is_windows:
        ctx.actions.run(
            inputs = [],
            outputs = [ebin],
            executable = "cp",
            arguments = [
                "-R",
                "{}/lib/elixir/ebin".format(elixir_home),
                ebin.dirname,
            ],
        )
    else:
        # robocopy exits non-zero when new files are copied, so we can't
        # just ctx.actions.run robocopy
        ctx.actions.run_shell(
            inputs = [],
            outputs = [ebin],
            command = "cp -R \"{elixir_home}\"/lib/elixir/ebin {ebin}".format(
                elixir_home = elixir_home,
                ebin = ebin.dirname,
            ),
        )

    return [
        DefaultInfo(
            files = depset([ebin]),
            runfiles = ctx.runfiles([ebin]),
        ),
        ErlangAppInfo(
            app_name = ctx.attr.name,
            erlang_version = erlang_version,
            include = [],
            beam = [ebin],
            priv = [],
            deps = [],
        ),
    ]

elixir_private = rule(
    implementation = _impl,
    attrs = {
        "is_windows": attr.bool(mandatory = True),
        "_erlang_version": attr.label(default = Label("@rules_erlang//:erlang_version")),
        "_erlang_home": attr.label(default = Label("@rules_erlang//:erlang_home")),
        "_elixir_home": attr.label(default = Label("//:elixir_home")),
    },
)

def elixir(**kwargs):
    elixir_private(
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        **kwargs
    )
