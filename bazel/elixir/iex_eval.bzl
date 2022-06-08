load(
    ":elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_copy_erlang",
)

def _impl(ctx):
    outs = [
        ctx.actions.declare_file(f)
        for f in ctx.attr.outs
    ]

    (erlang_home, _, erlang_runfiles) = erlang_dirs(ctx)
    (elixir_home, elixir_runfiles) = elixir_dirs(ctx)

    script = """set -euo pipefail

{maybe_copy_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}

export SRCS="{srcs}"
export OUTS="{outs}"

${{ABS_ELIXIR_HOME}}/bin/iex --eval "$1"
""".format(
        maybe_copy_erlang = maybe_copy_erlang(ctx),
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        srcs = ctx.configuration.host_path_separator.join([src.path for src in ctx.files.srcs]),
        outs = ctx.configuration.host_path_separator.join([out.path for out in outs]),
    )

    inputs = depset(
        direct = ctx.files.srcs,
        transitive = [
            erlang_runfiles.files,
            elixir_runfiles.files,
        ],
    )

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = outs,
        command = script,
        arguments = [ctx.attr.expression],
    )

    return [
        DefaultInfo(files = depset(outs)),
    ]

iex_eval = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "outs": attr.string_list(),
        "expression": attr.string(
            mandatory = True,
        ),
    },
    toolchains = [":toolchain_type"],
)
