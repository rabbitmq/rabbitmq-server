load("@bazel-erlang//:erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load(
    "@bazel-erlang//:bazel_erlang_lib.bzl",
    "BEGINS_WITH_FUN",
    "ErlangLibInfo",
    "QUERY_ERL_VERSION",
    "flat_deps",
    "path_join",
)
load("//:elixir_home.bzl", "ElixirHomeProvider")

MIX_DEPS_DIR = "mix_deps"

def _impl(ctx):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version
    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path
    elixir_home = ctx.attr._elixir_home[ElixirHomeProvider].path

    escript = ctx.actions.declare_file(path_join("escript", "rabbitmqctl"))
    ebin = ctx.actions.declare_directory("ebin")

    copy_compiled_deps_commands = []
    copy_compiled_deps_commands.append("mkdir ${{MIX_INVOCATION_DIR}}/{}".format(MIX_DEPS_DIR))
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        if lib_info.erlang_version != erlang_version:
            fail("Mismatched erlang versions", erlang_version, lib_info.erlang_version)

        dest_dir = path_join("${MIX_INVOCATION_DIR}", MIX_DEPS_DIR, lib_info.lib_name)
        copy_compiled_deps_commands.append(
            "mkdir {}".format(dest_dir),
        )
        copy_compiled_deps_commands.append(
            "mkdir {}".format(path_join(dest_dir, "include")),
        )
        copy_compiled_deps_commands.append(
            "mkdir {}".format(path_join(dest_dir, "ebin")),
        )
        for hdr in lib_info.include:
            copy_compiled_deps_commands.append(
                "cp ${{PWD}}/{source} {target}".format(
                    source = hdr.path,
                    target = path_join(dest_dir, "include", hdr.basename),
                ),
            )
        for beam in lib_info.beam:
            copy_compiled_deps_commands.append(
                "cp ${{PWD}}/{source} {target}".format(
                    source = beam.path,
                    target = path_join(dest_dir, "ebin", beam.basename),
                ),
            )

    mix_invocation_dir = ctx.actions.declare_directory("{}_mix".format(ctx.label.name))

    package_dir = ctx.label.package
    if ctx.label.workspace_root != "":
        package_dir = path_join(ctx.label.workspace_root, package_dir)

    script = """
        set -euo pipefail

        export LANG="en_US.UTF-8"
        export LC_ALL="en_US.UTF-8"

        export PATH={elixir_home}/bin:{erlang_home}/bin:${{PATH}}

        MIX_INVOCATION_DIR="{mix_invocation_dir}"

        cp -R ${{PWD}}/{package_dir}/config ${{MIX_INVOCATION_DIR}}/config
        # cp -R ${{PWD}}/{package_dir}/include ${{MIX_INVOCATION_DIR}}/include # rabbitmq_cli's include directory is empty
        cp -R ${{PWD}}/{package_dir}/lib ${{MIX_INVOCATION_DIR}}/lib
        cp    ${{PWD}}/{package_dir}/mix.exs ${{MIX_INVOCATION_DIR}}/mix.exs

        {copy_compiled_deps_command}

        cd ${{MIX_INVOCATION_DIR}}
        export HOME=${{PWD}}

        {begins_with_fun}
        V=$({query_erlang_version})
        if ! beginswith "{erlang_version}" "$V"; then
            echo "Erlang version mismatch (Expected {erlang_version}, found $V)"
            exit 1
        fi

        export DEPS_DIR={mix_deps_dir}
        export ERL_COMPILER_OPTIONS=deterministic
        mix local.hex --force
        mix local.rebar --force
        mix make_all

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
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        mix_invocation_dir = mix_invocation_dir.path,
        package_dir = package_dir,
        copy_compiled_deps_command = " && ".join(copy_compiled_deps_commands),
        mix_deps_dir = MIX_DEPS_DIR,
        escript_path = escript.path,
        ebin_dir = ebin.path,
    )

    inputs = []
    inputs.extend(ctx.files.srcs)
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        inputs.extend(lib_info.include)
        inputs.extend(lib_info.beam)

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [escript, ebin, mix_invocation_dir],
        command = script,
        mnemonic = "MIX",
    )

    deps = flat_deps(ctx.attr.deps)

    runfiles = ctx.runfiles([ebin])
    for dep in deps:
        runfiles = runfiles.merge(dep[DefaultInfo].default_runfiles)

    return [
        DefaultInfo(
            executable = escript,
            files = depset([ebin]),
            runfiles = runfiles,
        ),
        ErlangLibInfo(
            lib_name = ctx.attr.name,
            erlang_version = erlang_version,
            include = [],
            beam = [ebin],
            priv = [],
            deps = deps,
        ),
    ]

rabbitmqctl = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers = [ErlangLibInfo]),
        "_erlang_version": attr.label(default = "@bazel-erlang//:erlang_version"),
        "_erlang_home": attr.label(default = "@bazel-erlang//:erlang_home"),
        "_elixir_home": attr.label(default = "//:elixir_home"),
    },
    executable = True,
)
