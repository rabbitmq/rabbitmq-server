load("//bazel_erlang:erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load("//bazel_erlang:elixir_home.bzl", "ElixirHomeProvider")
load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo", "BEGINS_WITH_FUN", "QUERY_ERL_VERSION", "path_join")

MIX_DEPS_DIR = "mix_deps"

def _impl(ctx):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version
    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path
    elixir_home = ctx.attr._elixir_home[ElixirHomeProvider].path

    mix_invocation_dir = ctx.actions.declare_directory(
        path_join(
            ctx.label.name,
            "mix",
        )
    )
    escript = ctx.actions.declare_file(
        path_join(
            "escript",
            "rabbitmqctl",
        )
    )

    link_compiled_deps_commands = []
    link_compiled_deps_commands.append("mkdir {}/{}".format(mix_invocation_dir.path, MIX_DEPS_DIR))
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        if lib_info.erlang_version != erlang_version:
            fail("Mismatched erlang versions", erlang_version, lib_info.erlang_version)

        dest_dir = path_join(mix_invocation_dir.path, MIX_DEPS_DIR, lib_info.lib_name)
        link_compiled_deps_commands.append(
            "mkdir {}".format(dest_dir)
        )
        link_compiled_deps_commands.append(
            "mkdir {}".format(path_join(dest_dir, "include"))
        )
        link_compiled_deps_commands.append(
            "mkdir {}".format(path_join(dest_dir, "ebin"))
        )
        for hdr in lib_info.include:
            link_compiled_deps_commands.append(
                "ln -s ${{PWD}}/{source} {target}".format(
                    source = hdr.path,
                    target = path_join(dest_dir, "include", hdr.basename)
                )
            )
        for beam in lib_info.beam:
            link_compiled_deps_commands.append(
                "ln -s ${{PWD}}/{source} {target}".format(
                    source = beam.path,
                    target = path_join(dest_dir, "ebin", beam.basename)
                )
            )

    script = """
        set -euo pipefail

        export LANG="en_US.UTF-8"
        export LC_ALL="en_US.UTF-8"

        # In github actions, there is an erl at /usr/bin/erl...
        export PATH={elixir_home}/bin:{erlang_home}/bin:${{PATH}}

        mkdir -p {mix_invocation_dir}

        cp -R ${{PWD}}/{package_dir}/config {mix_invocation_dir}/config
        # cp -R ${{PWD}}/{package_dir}/include {mix_invocation_dir}/include # rabbitmq_cli's include directory is empty
        cp -R ${{PWD}}/{package_dir}/lib {mix_invocation_dir}/lib
        cp    ${{PWD}}/{package_dir}/mix.exs {mix_invocation_dir}/mix.exs

        {link_compiled_deps_command}

        cd {mix_invocation_dir}
        export HOME=${{PWD}}

        {begins_with_fun}
        V=$({query_erlang_version})
        if ! beginswith "{erlang_version}" "$V"; then
            echo "Erlang version mismatch (Expected {erlang_version}, found $V)"
            exit 1
        fi

        export DEPS_DIR={mix_deps_dir}
        mix local.hex --force
        mix local.rebar --force
        mix make_all

        cd ${{OLDPWD}}
        cp {mix_invocation_dir}/escript/rabbitmqctl {escript_path}
    """.format(
        begins_with_fun=BEGINS_WITH_FUN,
        query_erlang_version=QUERY_ERL_VERSION,
        erlang_version=erlang_version,
        erlang_home=erlang_home,
        elixir_home=elixir_home,
        mix_invocation_dir=mix_invocation_dir.path,
        package_dir=ctx.label.package,
        link_compiled_deps_command=" && ".join(link_compiled_deps_commands),
        mix_deps_dir=MIX_DEPS_DIR,
        escript_path=escript.path,
    )

    inputs = []
    inputs.extend(ctx.files.srcs)
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        inputs.extend(lib_info.include)
        inputs.extend(lib_info.beam)

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [mix_invocation_dir, escript],
        command = script,
    )

    return [DefaultInfo(
        executable = escript,
    )]

rabbitmqctl = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "_erlang_version": attr.label(default = "//bazel_erlang:erlang_version"),
        "_erlang_home": attr.label(default = "//bazel_erlang:erlang_home"),
        "_elixir_home": attr.label(default = "//bazel_erlang:elixir_home"),
    },
    executable = True,
)