load("@bazel-erlang//:erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load("@bazel-erlang//:elixir_home.bzl", "ElixirHomeProvider")
load("@bazel-erlang//:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")

def _impl(ctx):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version
    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path
    elixir_home = ctx.attr._elixir_home[ElixirHomeProvider].path

    ebin = ctx.actions.declare_directory(path_join(ctx.attr.name, "ebin"))

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

    return [
        DefaultInfo(
            files = depset([ebin]),
        ),
        ErlangLibInfo(
            lib_name = ctx.attr.name,
            lib_version = "0.0.0",
            erlang_version = erlang_version,
            include = [],
            beam = [ebin],
            priv = [],
        ),
    ]

elixir = rule(
    implementation = _impl,
    attrs = {
        "_erlang_version": attr.label(default = "@bazel-erlang//:erlang_version"),
        "_erlang_home": attr.label(default = "@bazel-erlang//:erlang_home"),
        "_elixir_home": attr.label(default = "@bazel-erlang//:elixir_home"),
    },
)
