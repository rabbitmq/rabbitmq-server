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
    "additional_file_dest_relative_path",
)
load(
    "//bazel/elixir:elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_install_erlang",
)

def deps_dir_contents(ctx, deps, dir):
    files = []
    for dep in deps:
        lib_info = dep[ErlangAppInfo]
        for src in lib_info.include + lib_info.beam + lib_info.srcs:
            rp = additional_file_dest_relative_path(dep.label, src)
            f = ctx.actions.declare_file(path_join(
                dir,
                lib_info.app_name,
                rp,
            ))
            ctx.actions.symlink(
                output = f,
                target_file = src,
            )
            files.append(f)
    return files

def _impl(ctx):
    (erlang_home, _, erlang_runfiles) = erlang_dirs(ctx)
    (elixir_home, elixir_runfiles) = elixir_dirs(ctx)

    escript = ctx.actions.declare_file(path_join("escript", "rabbitmqctl"))
    ebin = ctx.actions.declare_directory("ebin")
    mix_invocation_dir = ctx.actions.declare_directory("{}_mix".format(ctx.label.name))
    fetched_srcs = ctx.actions.declare_file("deps.tar")

    deps = flat_deps(ctx.attr.deps)

    deps_dir = ctx.label.name + "_deps"

    deps_dir_files = deps_dir_contents(ctx, deps, deps_dir)

    package_dir = path_join(
        ctx.label.workspace_root,
        ctx.label.package,
    )

    script = """set -euo pipefail

{maybe_install_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi
ABS_EBIN_DIR=$PWD/{ebin_dir}
ABS_ESCRIPT_PATH=$PWD/{escript_path}
ABS_FETCHED_SRCS=$PWD/{fetched_srcs}

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}

export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

MIX_INVOCATION_DIR="{mix_invocation_dir}"

cp -R {package_dir}/config ${{MIX_INVOCATION_DIR}}/config
cp -R {package_dir}/lib ${{MIX_INVOCATION_DIR}}/lib
cp    {package_dir}/mix.exs ${{MIX_INVOCATION_DIR}}/mix.exs

cd ${{MIX_INVOCATION_DIR}}
export IS_BAZEL=true
export HOME=${{PWD}}
export DEPS_DIR=$(dirname $ABS_EBIN_DIR)/{deps_dir}
export MIX_ENV=prod
export ERL_COMPILER_OPTIONS=deterministic
"${{ABS_ELIXIR_HOME}}"/bin/mix local.hex --force
"${{ABS_ELIXIR_HOME}}"/bin/mix local.rebar --force
"${{ABS_ELIXIR_HOME}}"/bin/mix deps.get
if [ ! -d _build/${{MIX_ENV}}/lib/rabbit_common ]; then
    cp -r ${{DEPS_DIR}}/* _build/${{MIX_ENV}}/lib
fi
"${{ABS_ELIXIR_HOME}}"/bin/mix deps.compile
"${{ABS_ELIXIR_HOME}}"/bin/mix compile
"${{ABS_ELIXIR_HOME}}"/bin/mix escript.build

cp escript/rabbitmqctl ${{ABS_ESCRIPT_PATH}}

cp _build/${{MIX_ENV}}/lib/rabbitmqctl/ebin/* ${{ABS_EBIN_DIR}}
cp _build/${{MIX_ENV}}/lib/rabbitmqctl/consolidated/* ${{ABS_EBIN_DIR}}

tar --file ${{ABS_FETCHED_SRCS}} \\
    --create deps

# remove symlinks from the _build directory since it
# is not used, and bazel does not allow them
find . -type l -delete
""".format(
        maybe_install_erlang = maybe_install_erlang(ctx),
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        mix_invocation_dir = mix_invocation_dir.path,
        package_dir = package_dir,
        deps_dir = deps_dir,
        escript_path = escript.path,
        ebin_dir = ebin.path,
        fetched_srcs = fetched_srcs.path,
    )

    inputs = depset(
        direct = ctx.files.srcs,
        transitive = [
            erlang_runfiles.files,
            elixir_runfiles.files,
            depset(deps_dir_files),
        ],
    )

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [escript, ebin, mix_invocation_dir, fetched_srcs],
        command = script,
        mnemonic = "MIX",
    )

    runfiles = ctx.runfiles([ebin]).merge_all([
        erlang_runfiles,
        elixir_runfiles,
    ] + [
        dep[DefaultInfo].default_runfiles
        for dep in deps
    ])

    return [
        DefaultInfo(
            executable = escript,
            files = depset([ebin, fetched_srcs]),
            runfiles = runfiles,
        ),
        ErlangAppInfo(
            app_name = "rabbitmq_cli",
            include = [],
            beam = [ebin],
            priv = [],
            license_files = ctx.files.license_files,
            srcs = ctx.files.srcs,
            deps = deps,
        ),
    ]

rabbitmqctl_private = rule(
    implementation = _impl,
    attrs = {
        "is_windows": attr.bool(
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
    executable = True,
)

def rabbitmqctl(**kwargs):
    rabbitmqctl_private(
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        **kwargs
    )
