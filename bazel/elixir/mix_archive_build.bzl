load(
    "@rules_erlang//:util.bzl",
    "path_join",
)
load(
    "@rules_erlang//private:util.bzl",
    "additional_file_dest_relative_path",
)
load(
    "//bazel/elixir:elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_install_erlang",
)

def _impl(ctx):
    (erlang_home, _, erlang_runfiles) = erlang_dirs(ctx)
    (elixir_home, elixir_runfiles) = elixir_dirs(ctx)

    out = ctx.actions.declare_file(ctx.attr.out.name)
    mix_invocation_dir = ctx.actions.declare_directory("{}_mix".format(ctx.label.name))

    package_dir = path_join(
        ctx.label.workspace_root,
        ctx.label.package,
    )

    copy_srcs_commands = []
    for src in ctx.files.srcs:
        dest = additional_file_dest_relative_path(ctx.label, src)
        copy_srcs_commands.extend([
            "mkdir -p $(dirname ${{MIX_INVOCATION_DIR}}/{dest})".format(
                dest = dest,
            ),
            "cp {flags}{src} ${{MIX_INVOCATION_DIR}}/{dest}".format(
                flags = "-r " if src.is_directory else "",
                src = src.path,
                dest = dest,
            ),
        ])

    script = """set -euo pipefail

{maybe_install_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi

ABS_OUT_PATH=$PWD/{out}

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}

export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

MIX_INVOCATION_DIR="{mix_invocation_dir}"

{copy_srcs_commands}

cd ${{MIX_INVOCATION_DIR}}
export HOME=${{PWD}}
export MIX_ENV=prod
export ERL_COMPILER_OPTIONS=deterministic
"${{ABS_ELIXIR_HOME}}"/bin/mix archive.build -o ${{ABS_OUT_PATH}}

# remove symlinks from the _build directory since it
# is an unused output, and bazel does not allow them
find . -type l -delete
""".format(
        maybe_install_erlang = maybe_install_erlang(ctx),
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        mix_invocation_dir = mix_invocation_dir.path,
        copy_srcs_commands = "\n".join(copy_srcs_commands),
        package_dir = package_dir,
        out = out.path,
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
        outputs = [
            out,
            mix_invocation_dir,
        ],
        command = script,
        mnemonic = "MIX",
    )

    return [
        DefaultInfo(
            files = depset([out]),
        ),
    ]

mix_archive_build = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(
            mandatory = True,
            allow_files = True,
        ),
        "out": attr.output(),
    },
    toolchains = [
        ":toolchain_type",
    ],
)
