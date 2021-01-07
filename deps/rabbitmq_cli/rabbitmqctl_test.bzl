load("//bazel_erlang:erlang_home.bzl", "ErlangHomeProvider")
load("//bazel_erlang:elixir_home.bzl", "ElixirHomeProvider", "MixArchivesProvider")
load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")
load("//bazel_erlang:ct.bzl", "lib_dir")
load(":rabbitmqctl.bzl", "MIX_DEPS_DIR")

def _impl(ctx):
    mix_archives = ctx.attr._mix_archives[MixArchivesProvider].path

    build_deps_dir_commands = []
    build_deps_dir_commands.append("mkdir -p {}".format(MIX_DEPS_DIR))
    for dep in ctx.attr.deps:
        build_deps_dir_commands.append("ln -s {source} {target}".format(
            # TODO: The next line is fragile w.r.t the rabbitmq_cli package path
            source = path_join("../..", "..", lib_dir(dep)),
            target = path_join(MIX_DEPS_DIR, dep[ErlangLibInfo].lib_name)
        ))

    script = """
    set -exuo pipefail
    cd {package_dir}
    export HOME=${{TEST_TMPDIR}}
    export MIX_ARCHIVES={mix_archives}
    {build_deps_dir_command}
    export DEPS_DIR={mix_deps_dir}

    cd ${{OLDPWD}} && tree && cd ${{OLDPWD}}

    mix local.rebar --force
    mix make_all

    # due to https://github.com/elixir-lang/elixir/issues/7699 we
    # "run" the tests, but skip them all, in order to trigger
    # compilation of all *_test.exs files before we actually run them
    mix test --exclude test

    # we need a running broker with certain plugins for this to pass... 
    # (check the github actions scripts for an example)
    # also look at the rabbitmq-server-copy dir to see where things are placed where cli has files
    ${{OLDPWD}}/{start_background_broker_script} PLUGINS="rabbitmq_federation rabbitmq_stomp"

    mix test
    """.format(
        package_dir=ctx.label.package,
        mix_archives=mix_archives,
        mix_deps_dir=MIX_DEPS_DIR,
        build_deps_dir_command=" && ".join(build_deps_dir_commands),
        start_background_broker_script=ctx.attr.start_background_broker_script.files.to_list()[0].short_path,
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(files = ctx.files.srcs).merge(
        ctx.runfiles(files = [dep[ErlangLibInfo].lib_parent for dep in ctx.attr.deps]),
    ).merge(
        ctx.runfiles(files = ctx.attr.start_background_broker_script.files.to_list())
    )

    return [DefaultInfo(runfiles = runfiles)]

rabbitmqctl_test = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers = [ErlangLibInfo]),
        "start_background_broker_script": attr.label(executable = True, cfg = "target"),
        "_erlang_home": attr.label(default = "//bazel_erlang:erlang_home"),
        "_elixir_home": attr.label(default = "//bazel_erlang:elixir_home"),
        "_mix_archives": attr.label(default = "//bazel_erlang:mix_archives"),
    },
    test = True,
)