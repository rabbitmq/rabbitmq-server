load("@bazel-erlang//:erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load("@bazel-erlang//:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")
load("//:elixir_home.bzl", "ElixirHomeProvider")

def _impl(ctx):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version
    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path
    elixir_home = ctx.attr._elixir_home[ElixirHomeProvider].path

    ebin = ctx.actions.declare_directory(path_join(ctx.attr.name, "ebin"))

    ctx.actions.run_shell(
        inputs = [],
        outputs = [ebin],
        command = "mkdir -p {ebin_dirname} && cp -R {elixir_home}/lib/elixir/ebin {ebin_dirname}".format(
            ebin_dirname = ebin.dirname,
            elixir_home = elixir_home,
        ),
    )

    return [
        DefaultInfo(
            files = depset([ebin]),
            runfiles = ctx.runfiles([ebin]),
        ),
        ErlangLibInfo(
            lib_name = ctx.attr.name,
            erlang_version = erlang_version,
            include = [],
            beam = [ebin],
            priv = [],
            deps = [],
        ),
    ]

elixir = rule(
    implementation = _impl,
    attrs = {
        "_erlang_version": attr.label(default = "@bazel-erlang//:erlang_version"),
        "_erlang_home": attr.label(default = "@bazel-erlang//:erlang_home"),
        "_elixir_home": attr.label(default = "//:elixir_home"),
    },
)
