load("@rules_erlang//:erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load("@rules_erlang//:bazel_erlang_lib.bzl", "BEGINS_WITH_FUN", "ErlangLibInfo", "QUERY_ERL_VERSION", "path_join")
load("@rules_erlang//:ct.bzl", "code_paths")
load("//:elixir_home.bzl", "ElixirHomeProvider")
load(":rabbitmqctl.bzl", "MIX_DEPS_DIR")

def _lib_dirs(dep):
    return [path_join(p, "..") for p in code_paths(dep)]

def _impl(ctx):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version
    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path
    elixir_home = ctx.attr._elixir_home[ElixirHomeProvider].path

    copy_compiled_deps_commands = []
    copy_compiled_deps_commands.append("mkdir ${{TEST_UNDECLARED_OUTPUTS_DIR}}/{}".format(MIX_DEPS_DIR))
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        if lib_info.erlang_version != erlang_version:
            fail("Mismatched erlang versions", erlang_version, lib_info.erlang_version)

        dest_dir = path_join("${TEST_UNDECLARED_OUTPUTS_DIR}", MIX_DEPS_DIR, lib_info.lib_name)
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
                    source = hdr.short_path,
                    target = path_join(dest_dir, "include", hdr.basename),
                ),
            )
        for beam in lib_info.beam:
            copy_compiled_deps_commands.append(
                "cp ${{PWD}}/{source} {target}".format(
                    source = beam.short_path,
                    target = path_join(dest_dir, "ebin", beam.basename),
                ),
            )

    erl_libs = ":".join(
        [path_join("${TEST_SRCDIR}/${TEST_WORKSPACE}", d) for dep in ctx.attr.deps for d in _lib_dirs(dep)],
    )

    script = """
        set -euo pipefail

        export LANG="en_US.UTF-8"
        export LC_ALL="en_US.UTF-8"

        export PATH={elixir_home}/bin:{erlang_home}/bin:${{PATH}}

        INITIAL_DIR=${{PWD}}

        ln -s ${{PWD}}/{package_dir}/config ${{TEST_UNDECLARED_OUTPUTS_DIR}}
        # ln -s ${{PWD}}/{package_dir}/include ${{TEST_UNDECLARED_OUTPUTS_DIR}}
        ln -s ${{PWD}}/{package_dir}/lib ${{TEST_UNDECLARED_OUTPUTS_DIR}}
        ln -s ${{PWD}}/{package_dir}/test ${{TEST_UNDECLARED_OUTPUTS_DIR}}
        ln -s ${{PWD}}/{package_dir}/mix.exs ${{TEST_UNDECLARED_OUTPUTS_DIR}}

        {copy_compiled_deps_command}

        cd ${{TEST_UNDECLARED_OUTPUTS_DIR}}

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
        mix make_deps

        # due to https://github.com/elixir-lang/elixir/issues/7699 we
        # "run" the tests, but skip them all, in order to trigger
        # compilation of all *_test.exs files before we actually run them
        mix test --exclude test

        export TEST_TMPDIR=${{TEST_UNDECLARED_OUTPUTS_DIR}}

        # we need a running broker with certain plugins for this to pass 
        trap 'catch $?' EXIT
        catch() {{
            pid=$(cat ${{TEST_TMPDIR}}/*/*.pid)
            kill -TERM "${{pid}}"
        }}
        cd ${{INITIAL_DIR}}
        ./{rabbitmq_run_cmd} start-background-broker
        cd ${{TEST_UNDECLARED_OUTPUTS_DIR}}

        # The test cases will need to be able to load code from the deps
        # directly, so we set ERL_LIBS
        export ERL_LIBS={erl_libs}

        # run the actual tests
        mix test --trace --max-failures 1
    """.format(
        begins_with_fun = BEGINS_WITH_FUN,
        query_erlang_version = QUERY_ERL_VERSION,
        erlang_version = erlang_version,
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        package_dir = ctx.label.package,
        copy_compiled_deps_command = " && ".join(copy_compiled_deps_commands),
        mix_deps_dir = MIX_DEPS_DIR,
        erl_libs = erl_libs,
        rabbitmq_run_cmd = ctx.attr.rabbitmq_run[DefaultInfo].files_to_run.executable.short_path,
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(ctx.files.srcs)
    runfiles = runfiles.merge(ctx.runfiles(ctx.files.data))
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        runfiles = runfiles.merge(ctx.runfiles(lib_info.include + lib_info.beam))
    runfiles = runfiles.merge(ctx.attr.rabbitmq_run[DefaultInfo].default_runfiles)

    return [DefaultInfo(runfiles = runfiles)]

rabbitmqctl_test = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".ex", ".exs"]),
        "data": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers = [ErlangLibInfo]),
        "rabbitmq_run": attr.label(
            executable = True,
            cfg = "target",
        ),
        "_erlang_version": attr.label(default = "@rules_erlang//:erlang_version"),
        "_erlang_home": attr.label(default = "@rules_erlang//:erlang_home"),
        "_elixir_home": attr.label(default = "//:elixir_home"),
    },
    test = True,
)
