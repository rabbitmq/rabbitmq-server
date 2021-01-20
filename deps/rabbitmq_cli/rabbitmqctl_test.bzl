load("//bazel_erlang:erlang_home.bzl", "ErlangHomeProvider")
load("//bazel_erlang:elixir_home.bzl", "ElixirHomeProvider", "MixArchivesProvider")
load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")
load("//bazel_erlang:ct.bzl", "lib_dir")
load(":rabbitmqctl.bzl", "MIX_DEPS_DIR")

def _impl(ctx):
    build_deps_dir_commands = []
    build_deps_dir_commands.append("mkdir -p {}".format(MIX_DEPS_DIR))
    for dep in ctx.attr.deps:
        build_deps_dir_commands.append("ln -s {source} {target}".format(
            # TODO: The next line is fragile w.r.t the rabbitmq_cli package path
            source = path_join("../..", "..", lib_dir(dep)),
            target = path_join(MIX_DEPS_DIR, dep[ErlangLibInfo].lib_name)
        ))

    erl_libs = ":".join(
        [path_join("../..", lib_dir(dep)) for dep in ctx.attr.deps]
    )

    script = """
    set -exuo pipefail

    PATH={elixir_home}/bin:{erlang_home}/bin:${{PATH}}

    INITIAL_DIR=${{PWD}}
    cd {package_dir}
    export HOME=${{TEST_TMPDIR}}
    export MIX_ARCHIVES={mix_archives}
    {build_deps_dir_command}
    export DEPS_DIR={mix_deps_dir}

    mix local.rebar --force
    mix make_deps

    # cd ${{INITIAL_DIR}} && tree && cd {package_dir}

    # The test cases will need to be able to load code from the deps
    # directly, so we set ERL_LIBS
    export ERL_LIBS={erl_libs}

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
    ./{start_background_broker_cmd}
    cd {package_dir}

    # run the actual tests
    mix test --trace --max-failures 1
    """.format(
        package_dir=ctx.label.package,
        erlang_home=ctx.attr._erlang_home[ErlangHomeProvider].path,
        elixir_home=ctx.attr._elixir_home[ElixirHomeProvider].path,
        mix_archives=ctx.attr._mix_archives[MixArchivesProvider].path,
        mix_deps_dir=MIX_DEPS_DIR,
        build_deps_dir_command=" && ".join(build_deps_dir_commands),
        erl_libs=erl_libs,
        start_background_broker_cmd=ctx.attr._start_background_broker.files.to_list()[0].short_path,
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(ctx.files.srcs)
    runfiles = runfiles.merge(ctx.runfiles(ctx.files.data))
    runfiles = runfiles.merge(
        ctx.runfiles(
            files = [dep[ErlangLibInfo].lib_dir for dep in ctx.attr.deps],
        ),
    )
    runfiles = runfiles.merge(ctx.attr._start_background_broker[DefaultInfo].default_runfiles)

    return [DefaultInfo(runfiles = runfiles)]

rabbitmqctl_test = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".ex", ".exs"]),
        "data": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers = [ErlangLibInfo]),
        "_start_background_broker": attr.label(
            default = Label("//:broker-for-cli-tests"),
        ),
        "_erlang_home": attr.label(default = "//bazel_erlang:erlang_home"),
        "_elixir_home": attr.label(default = "//bazel_erlang:elixir_home"),
        "_mix_archives": attr.label(default = "//bazel_erlang:mix_archives"),
    },
    test = True,
)