load("//bazel_erlang:erlang_home.bzl", "ErlangHomeProvider")
load("//bazel_erlang:elixir_home.bzl", "ElixirHomeProvider", "MixArchivesProvider")
load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")

MIX_DEPS_DIR = "mix_deps"

def _impl(ctx):
    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path
    elixir_home = ctx.attr._elixir_home[ElixirHomeProvider].path
    mix_archives = ctx.attr._mix_archives[MixArchivesProvider].path

    home_dir = ctx.actions.declare_directory("home")
    ctx.actions.run(
        outputs = [home_dir],
        executable = "mkdir",
        arguments = [home_dir.path],
    )

    escript = ctx.actions.declare_file(path_join("escript", ctx.attr.name))

    build_cmds = []
    # build_cmds.append("find .")
    build_cmds.append("cd {}".format(ctx.label.package))
    build_cmds.append("mkdir -p {}".format(MIX_DEPS_DIR))
    for dep in ctx.attr.deps:
        info = dep[ErlangLibInfo]
        build_cmds.append("ln -s {source} {target}".format(
            # TODO: The next line is fragile w.r.t the rabbitmq_cli package path
            source = path_join("../..", "..", info.lib_dir.path),
            target = path_join(MIX_DEPS_DIR, info.lib_name)
        ))
    build_cmds.append("mix local.rebar --force")
    build_cmds.append("mix $@")
    build_cmds.append("mkdir -p $OLDPWD/{0} && mv escript/* $OLDPWD/{0}".format(escript.dirname))

    cmds = [
        "export PATH=$PATH:{}/bin:{}/bin".format(erlang_home, elixir_home),
        "set -x",
        # "printenv",
        # "/usr/local/bin/tree",
        " && ".join(build_cmds),
    ]

    ctx.actions.run_shell(
        inputs = ctx.files.srcs + [dep[ErlangLibInfo].lib_dir for dep in ctx.attr.deps],
        outputs = [escript],
        command = "; ".join(cmds),
        arguments = [ctx.attr.command],
        env = {
            "HOME": home_dir.path,
            "MIX_ARCHIVES": mix_archives,
            "DEPS_DIR": MIX_DEPS_DIR,
        },
    )

    return [DefaultInfo(
        executable = escript,
    )]

rabbitmqctl = rule(
    implementation = _impl,
    attrs = {
        "command": attr.string(),
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "_erlang_home": attr.label(default = "//bazel_erlang:erlang_home"),
        "_elixir_home": attr.label(default = "//bazel_erlang:elixir_home"),
        "_mix_archives": attr.label(default = "//bazel_erlang:mix_archives"),
    },
    executable = True,
)