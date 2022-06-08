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
    "//bazel/mix:mix_app.bzl",
    "find_mix_exs",
)
load(
    "//bazel/elixir:elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_install_erlang",
)

def _impl(ctx):
    mix_exs = find_mix_exs(ctx.files.srcs)

    escript = ctx.actions.declare_file(path_join("escript", ctx.label.name))
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

    package_dir = path_join(ctx.label.workspace_root, ctx.label.package)

    script = """set -euo pipefail

export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

{maybe_install_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi
ABS_EBIN_DIR=$PWD/{ebin}
ABS_ESCRIPT_PATH=$PWD/{escript_path}

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}
export HOME=$PWD/{home}
export MIX_BUILD_PATH=$PWD/{build_dir}
export MIX_ENV=prod
export DEPS_DIR=$(dirname ${{ABS_EBIN_DIR}})/{erl_libs_dir}
export ERL_LIBS=${{DEPS_DIR}}

cd $(dirname {mix_exs})

export ERL_COMPILER_OPTIONS=deterministic
"${{ABS_ELIXIR_HOME}}"/bin/mix compile --no-deps-check
"${{ABS_ELIXIR_HOME}}"/bin/mix escript.build --no-deps-check

mv escript/rabbitmqctl ${{ABS_ESCRIPT_PATH}}
if [ -n "$(ls ${{MIX_BUILD_PATH}}/lib/{app_name}/consolidated)" ]; then
    cp ${{MIX_BUILD_PATH}}/lib/{app_name}/consolidated/* ${{ABS_EBIN_DIR}}
fi
<<<<<<< HEAD

export DEPS_DIR={mix_deps_dir}

# mix can error on windows regarding permissions for a symlink at this path
# deps/rabbitmq_cli/rabbitmqctl_mix/_build/dev/lib/rabbit_common/ebin
# so instead we'll try skip that
mkdir -p _build/dev/lib/rabbit_common
mkdir _build/dev/lib/rabbit_common/include
cp ${{DEPS_DIR}}/rabbit_common/include/* \\
    _build/dev/lib/rabbit_common/include
mkdir _build/dev/lib/rabbit_common/ebin
cp ${{DEPS_DIR}}/rabbit_common/ebin/* \\
    _build/dev/lib/rabbit_common/ebin

mkdir -p _build/dev/lib/lager
mkdir _build/dev/lib/lager/include
cp ${{DEPS_DIR}}/lager/include/* \\
    _build/dev/lib/lager/include
mkdir _build/dev/lib/lager/ebin
cp ${{DEPS_DIR}}/lager/ebin/* \\
    _build/dev/lib/lager/ebin

mkdir -p _build/dev/lib/goldrush
mkdir _build/dev/lib/goldrush/ebin
cp ${{DEPS_DIR}}/goldrush/ebin/* \\
    _build/dev/lib/goldrush/ebin

export ERL_COMPILER_OPTIONS=deterministic
"{elixir_home}"/bin/mix local.hex --force
"{elixir_home}"/bin/mix local.rebar --force
"{elixir_home}"/bin/mix make_all_in_src_archive

cd ${{OLDPWD}}
cp ${{MIX_INVOCATION_DIR}}/escript/rabbitmqctl {escript_path}

mkdir -p {ebin_dir}
mv ${{MIX_INVOCATION_DIR}}/_build/dev/lib/rabbitmqctl/ebin/* {ebin_dir}
mv ${{MIX_INVOCATION_DIR}}/_build/dev/lib/rabbitmqctl/consolidated/* {ebin_dir}

rm -dR ${{MIX_INVOCATION_DIR}}
mkdir ${{MIX_INVOCATION_DIR}}
touch ${{MIX_INVOCATION_DIR}}/placeholder
    """.format(
        begins_with_fun = BEGINS_WITH_FUN,
        query_erlang_version = QUERY_ERL_VERSION,
        erlang_version = erlang_version,
=======
if [ -n "$(ls ${{MIX_BUILD_PATH}}/lib/{app_name}/ebin)" ]; then
    cp ${{MIX_BUILD_PATH}}/lib/{app_name}/ebin/* ${{ABS_EBIN_DIR}}
fi
""".format(
        maybe_install_erlang = maybe_install_erlang(ctx),
>>>>>>> 82997f7ff6 (Use rules_erlang 3 (backport #4884) (backport #5000) (#5002))
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        home = home.path,
        build_dir = build_dir.path,
        package_dir = package_dir,
        erl_libs_dir = erl_libs_dir,
        mix_exs = mix_exs.path,
        app_name = "rabbitmqctl",
        escript_path = escript.path,
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
        outputs = [escript, ebin, home, build_dir],
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
            executable = escript,
            files = depset([ebin]),
            runfiles = runfiles,
        ),
        ErlangAppInfo(
            app_name = ctx.attr.name,
            include = [],
            beam = [ebin],
            priv = [],
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
