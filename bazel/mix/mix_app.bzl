load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
    "flat_deps",
)
load(
    "@rules_erlang//:util.bzl",
    "path_join",
)
load(
    "@rules_erlang//private:util.bzl",
    "erl_libs_contents",
)
load(
    "//bazel/elixir:elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_symlink_erlang",
)

def find_mix_exs(files):
    mix_exs = [
        f
        for f in files
        if f.basename == "mix.exs"
    ]
    if len(mix_exs) == 0:
        fail("mix.exs is not among the srcs")
    return mix_exs[0]

def _mix_deps_dir_contents(ctx, deps_dir):
    files = []
    for (srcs, name) in ctx.attr.dep_srcs.items():
        for src in srcs.files.to_list():
            rp = src.path.removeprefix("external/.elixir.{}/".format(name))
            f = ctx.actions.declare_file(path_join(deps_dir, name, rp))
            ctx.actions.symlink(
                output = f,
                target_file = src,
            )
            files.append(f)
    return files

def _impl(ctx):
    mix_exs = find_mix_exs(ctx.files.srcs)

    ebin = ctx.actions.declare_directory("ebin")
    home = ctx.actions.declare_directory("mix_home")
    build_dir = ctx.actions.declare_directory("_build")

    (erlang_home, _, erlang_runfiles) = erlang_dirs(ctx)
    (elixir_home, elixir_runfiles) = elixir_dirs(ctx)

    erl_libs_dir = ctx.label.name + "_apps"
    erl_libs_files = erl_libs_contents(
        ctx,
        headers = True,
        dir = erl_libs_dir,
    )

    script = """set -euo pipefail

{maybe_symlink_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi
ABS_EBIN_DIR=$PWD/{ebin}

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}
export HOME=$PWD/{home}
export MIX_BUILD_PATH=$PWD/{build_dir}
export MIX_ENV=prod
export DEPS_DIR=$(dirname ${{ABS_EBIN_DIR}})/{erl_libs_dir}
export ERL_LIBS=${{DEPS_DIR}}

cd $(dirname {mix_exs})

export ERL_COMPILER_OPTIONS=deterministic
${{ABS_ELIXIR_HOME}}/bin/mix compile --no-deps-check

if [ -n "$(ls ${{MIX_BUILD_PATH}}/lib/{app_name}/consolidated)" ]; then
    cp ${{MIX_BUILD_PATH}}/lib/{app_name}/consolidated/* ${{ABS_EBIN_DIR}}
fi
if [ -n "$(ls ${{MIX_BUILD_PATH}}/lib/{app_name}/ebin)" ]; then
    cp ${{MIX_BUILD_PATH}}/lib/{app_name}/ebin/* ${{ABS_EBIN_DIR}}
fi

# remove symlinks from the _build directory since it
# is not used, and bazel does not allow them
find ${{MIX_BUILD_PATH}} -type l -delete
""".format(
        maybe_symlink_erlang = maybe_symlink_erlang(ctx),
        package = ctx.label.package,
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        home = home.path,
        build_dir = build_dir.path,
        erl_libs_dir = erl_libs_dir,
        mix_exs = mix_exs.path,
        app_name = ctx.attr.app_name,
        ebin = ebin.path,
    )

    inputs = depset(
        direct = ctx.files.srcs,
        transitive = [
            erlang_runfiles.files,
            elixir_runfiles.files,
            depset(erl_libs_files),
        ],
    )

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [ebin, home, build_dir],
        command = script,
        mnemonic = "MIX",
    )

    deps = flat_deps(ctx.attr.deps)

    runfiles = ctx.runfiles([ebin])
    runfiles = runfiles.merge_all(
        [
            erlang_runfiles,
            elixir_runfiles,
        ] + [
            dep[DefaultInfo].default_runfiles
            for dep in deps
        ],
    )

    return [
        DefaultInfo(
            files = depset([ebin]),
            runfiles = runfiles,
        ),
        ErlangAppInfo(
            app_name = ctx.attr.app_name,
            include = [],
            beam = [ebin],
            priv = [],
            license_files = ctx.files.license_files,
            deps = deps,
        ),
    ]

mix_app = rule(
    implementation = _impl,
    attrs = {
        "app_name": attr.string(
            mandatory = True,
        ),
        "srcs": attr.label_list(
            mandatory = True,
            allow_files = True,
        ),
        "license_files": attr.label_list(
            allow_files = True,
        ),
        "deps": attr.label_list(
            providers = [ErlangAppInfo],
        ),
    },
    toolchains = [
        "//bazel/elixir:toolchain_type",
    ],
    provides = [ErlangAppInfo],
)
