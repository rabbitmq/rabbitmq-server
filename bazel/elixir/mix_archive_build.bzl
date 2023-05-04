load("@bazel_skylib//lib:shell.bzl", "shell")
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

    copy_srcs_commands = []
    for src in ctx.attr.srcs:
        for src_file in src[DefaultInfo].files.to_list():
            dest = additional_file_dest_relative_path(src.label, src_file)
            copy_srcs_commands.extend([
                'mkdir -p "$(dirname ${{MIX_INVOCATION_DIR}}/{dest})"'.format(
                    dest = dest,
                ),
                'cp {flags}"{src}" "${{MIX_INVOCATION_DIR}}/{dest}"'.format(
                    flags = "-r " if src_file.is_directory else "",
                    src = src_file.path,
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

ABS_OUT_PATH="$PWD/{out}"

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}

export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

MIX_INVOCATION_DIR="{mix_invocation_dir}"

{copy_srcs_commands}

ORIGINAL_DIR=$PWD
cd "${{MIX_INVOCATION_DIR}}"
export HOME="${{PWD}}"
export MIX_ENV=prod
export ERL_COMPILER_OPTIONS=deterministic
for archive in {archives}; do
    "${{ABS_ELIXIR_HOME}}"/bin/mix archive.install --force $ORIGINAL_DIR/$archive
done
if [[ -n "{ez_deps}" ]]; then
    mkdir -p _build/${{MIX_ENV}}/lib
    for ez_dep in {ez_deps}; do
        unzip -q $ORIGINAL_DIR/$ez_dep -d _build/${{MIX_ENV}}/lib
    done
fi
"${{ABS_ELIXIR_HOME}}"/bin/mix archive.build \\
    --no-deps-check \\
    -o "${{ABS_OUT_PATH}}"

# remove symlinks from the _build directory since it
# is an unused output, and bazel does not allow them
find . -type l -delete
""".format(
        maybe_install_erlang = maybe_install_erlang(ctx),
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        mix_invocation_dir = mix_invocation_dir.path,
        copy_srcs_commands = "\n".join(copy_srcs_commands),
        archives = " ".join([shell.quote(a.path) for a in ctx.files.archives]),
        ez_deps = " ".join([shell.quote(a.path) for a in ctx.files.ez_deps]),
        out = out.path,
    )

    inputs = depset(
        direct = ctx.files.srcs,
        transitive = [
            erlang_runfiles.files,
            elixir_runfiles.files,
            depset(ctx.files.archives),
            depset(ctx.files.ez_deps),
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
        "archives": attr.label_list(
            allow_files = [".ez"],
        ),
        "ez_deps": attr.label_list(
            allow_files = [".ez"],
        ),
        "out": attr.output(),
    },
    toolchains = [
        ":toolchain_type",
    ],
)
