load("//bazel_erlang:erlang_home.bzl", "ErlangVersionProvider", "ErlangHomeProvider")
load("//bazel_erlang:elixir_home.bzl", "ElixirHomeProvider", "MixArchivesProvider")
load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")

MIX_DEPS_DIR = "mix_deps"

def _impl(ctx):
    erlang_version = ctx.attr.erlang_version
    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path
    elixir_home = ctx.attr._elixir_home[ElixirHomeProvider].path
    mix_archives = ctx.attr._mix_archives[MixArchivesProvider].path

    mix_invocation_dir = ctx.actions.declare_directory(
        path_join(
            ctx.label.name,
            "mix",
        )
    )
    escript = ctx.actions.declare_file(
        path_join(
            ctx.label.name,
            "escript",
            "rabbitmqctl",
        )
    )

    # when linked instead of copied, we encounter a bazel error such as
    # "A TreeArtifact may not contain relative symlinks whose target paths traverse outside of the TreeArtifact"
    copy_compiled_deps_commands = []
    copy_compiled_deps_commands.append("mkdir {}/{}".format(mix_invocation_dir.path, MIX_DEPS_DIR))
    for dep in ctx.attr.deps:
        info = dep[ErlangLibInfo]
        copy_compiled_deps_commands.append(
            "cp -R ${{PWD}}/{source} {target}".format(
                source = info.lib_dir.path,
                target = path_join(mix_invocation_dir.path, MIX_DEPS_DIR, info.lib_name)
            )
        )

    script = """
        set -euxo pipefail

        export LANG="en_US.UTF-8"
        export LC_ALL="en_US.UTF-8"

        export PATH=${{PATH}}:{erlang_home}/bin:{elixir_home}/bin

        mkdir -p {mix_invocation_dir}

        cp -R ${{PWD}}/{package_dir}/config {mix_invocation_dir}/config
        # cp -R ${{PWD}}/{package_dir}/include {mix_invocation_dir}/include # rabbitmq_cli's include directory is empty
        cp -R ${{PWD}}/{package_dir}/lib {mix_invocation_dir}/lib
        cp    ${{PWD}}/{package_dir}/mix.exs {mix_invocation_dir}/mix.exs

        {copy_compiled_deps_command}

        cd {mix_invocation_dir}
        export HOME=${{PWD}}
        export DEPS_DIR={mix_deps_dir}
        mix local.rebar --force
        mix make_all

        cd ${{OLDPWD}}
        cp {mix_invocation_dir}/escript/rabbitmqctl {escript_path}
    """.format(
        erlang_home=erlang_home,
        elixir_home=elixir_home,
        mix_invocation_dir=mix_invocation_dir.path,
        package_dir=ctx.label.package,
        copy_compiled_deps_command=" && ".join(copy_compiled_deps_commands),
        mix_deps_dir=MIX_DEPS_DIR,
        escript_path=escript.path,
    )

    ctx.actions.run_shell(
        inputs = ctx.files.srcs + [dep[ErlangLibInfo].lib_dir for dep in ctx.attr.deps],
        outputs = [mix_invocation_dir, escript],
        command = script,
        env = {
            "MIX_ARCHIVES": mix_archives,
            "ERLANG_VERSION": erlang_version,
        },
    )

    return [DefaultInfo(
        executable = escript,
    )]

rabbitmqctl = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "erlang_version": attr.string(),
        "_erlang_home": attr.label(default = "//bazel_erlang:erlang_home"),
        "_elixir_home": attr.label(default = "//bazel_erlang:elixir_home"),
        "_mix_archives": attr.label(default = "//bazel_erlang:mix_archives"),
    },
    # Should we used named outputs here?
    executable = True,
)