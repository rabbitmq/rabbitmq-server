load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
)
load(
    "@rules_erlang//:util.bzl",
    "path_join",
)
load(
    ":elixir_toolchain.bzl",
    "elixir_dirs",
)

def _impl(ctx):
    ebin = ctx.actions.declare_directory(path_join(ctx.label.name, "ebin"))

    (elixir_home, elixir_runfiles) = elixir_dirs(ctx)

    ctx.actions.run_shell(
        inputs = elixir_runfiles.files,
        outputs = [ebin],
        command = """set -euo pipefail

cp -r "{elixir_home}"/lib/{app}/ebin/* {ebin}
""".format(
            elixir_home = elixir_home,
            app = ctx.attr.app,
            ebin = ebin.path,
        ),
    )

    return [
        DefaultInfo(files = depset([ebin])),
        ErlangAppInfo(
            app_name = ctx.attr.app,
            include = [],
            beam = [ebin],
            priv = [],
            license_files = [],
            srcs = [],
            deps = [],
        ),
    ]

elixir_as_app = rule(
    implementation = _impl,
    attrs = {
        "app": attr.string(default = "elixir"),
    },
    toolchains = [":toolchain_type"],
    provides = [ErlangAppInfo],
)
