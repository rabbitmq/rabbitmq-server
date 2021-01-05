load("//bazel_erlang:erlang_home.bzl", "ErlangHomeProvider")
load("//bazel_erlang:elixir_home.bzl", "ElixirHomeProvider", "MixArchivesProvider")
load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo")

MIX_DEPS_DIR = "mix_deps"

def dep_path(dep):
    c = []
    c.append(dep.label.workspace_root) if dep.label.workspace_root != "" else None
    c.append(dep.label.package) if dep.label.package != "" else None
    return "/".join(c)

def deps_files(deps):
    l = [[dep[ErlangLibInfo].beam_files, dep[ErlangLibInfo].hdrs] for dep in deps]
    return [item for sublist in l for item in sublist]

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

    build_cmds = []
    # build_cmds.append("find .")
    build_cmds.append("cd {}".format(ctx.label.package))
    for dep in ctx.attr.deps:
        build_cmds.append("mkdir -p {}/{}/include".format(MIX_DEPS_DIR, dep[ErlangLibInfo].name))
        for header in dep[ErlangLibInfo].hdrs.to_list():
            build_cmds.append("ln -s {source} {target}".format(
                # TODO: The next line is fragile w.r.t the rabbitmq_cli package path
                source = "/".join(["../../../../..", header.path]),
                target = "{}/{}/include/{}".format(MIX_DEPS_DIR, dep[ErlangLibInfo].name, header.basename)
            ))
        build_cmds.append("mkdir -p {}/{}/ebin".format(MIX_DEPS_DIR, dep[ErlangLibInfo].name))
        for beam in dep[ErlangLibInfo].beam_files.to_list():
            build_cmds.append("ln -s {source} {target}".format(
                # TODO: The next line is fragile w.r.t the rabbitmq_cli package path
                source = "/".join(["../../../../..", beam.path]),
                target = "{}/{}/ebin/{}".format(MIX_DEPS_DIR, dep[ErlangLibInfo].name, beam.basename)
            ))
    build_cmds.append("mix local.rebar --force")
    build_cmds.append("mix $@")
    build_cmds.append("mkdir -p $OLDPWD/{0} && mv escript/* $OLDPWD/{0}".format(ctx.outputs.executable.dirname))

    cmds = [
        "export PATH=$PATH:{}/bin:{}/bin".format(erlang_home, elixir_home),
        "set -x",
        # "printenv",
        # "find .",
        " && ".join(build_cmds),
    ]

    ctx.actions.run_shell(
        inputs = depset(
            direct = ctx.files.srcs,
            transitive = deps_files(ctx.attr.deps),
        ),
        outputs = [ctx.outputs.executable],
        command = "; ".join(cmds),
        arguments = [ctx.attr.command],
        env = {
            "HOME": home_dir.path,
            "MIX_ARCHIVES": mix_archives,
            "DEPS_DIR": MIX_DEPS_DIR,
        },
    )

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