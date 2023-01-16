load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
)
load(
    ":elixir_toolchain.bzl",
    "elixir_dirs",
)

def _impl(ctx):
    ebin = ctx.actions.declare_directory("ebin")

    (elixir_home, elixir_runfiles) = elixir_dirs(ctx)

    ctx.actions.run_shell(
        inputs = elixir_runfiles.files,
        outputs = [ebin],
        command = """set -euo pipefail

cp -r "{elixir_home}"/lib/elixir/ebin/* {ebin}
""".format(
            elixir_home = elixir_home,
            ebin = ebin.path,
        ),
    )

    return [
        DefaultInfo(files = depset([ebin])),
        ErlangAppInfo(
            app_name = "elixir",
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
    toolchains = [":toolchain_type"],
    provides = [ErlangAppInfo],
)
