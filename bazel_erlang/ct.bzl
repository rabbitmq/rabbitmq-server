load(":erlang_home.bzl", "ErlangHomeProvider")
load(":bazel_erlang_lib.bzl", "ErlangLibInfo", "BEGINS_WITH_FUN", "QUERY_ERL_VERSION", "path_join", "compile_erlang_action")

def lib_dir(dep):
    c = []
    c.append(dep.label.workspace_root) if dep.label.workspace_root != "" else None
    c.append(dep[ErlangLibInfo].lib_dir.short_path)
    return path_join(*c)

def ebin_dir(dep):
    return path_join(lib_dir(dep), "ebin")

def sanitize_sname(s):
    return s.replace("@", "-").replace(".", "_")

def _impl(ctx):
    erlang_lib_info = compile_erlang_action(
        ctx, 
        srcs=ctx.files.suites, 
        hdrs=ctx.files.hdrs,
        gen_app_file=False,
    )

    pa_args = " ".join(
        ["-pa {}".format(ebin_dir(dep)) for dep in ctx.attr.deps + ctx.attr.runtime_deps]
    )

    test_env_commands = []
    for k, v in ctx.attr.test_env.items():
        test_env_commands.append("export {}=\"{}\"".format(k, v))

    script = """set -euo pipefail

    export HOME=${{TEST_TMPDIR}}

    {begins_with_fun}
    V=$({erlang_home}/bin/{query_erlang_version})
    if ! beginswith "{erlang_version}" "$V"; then
        echo "Erlang version mismatch (Expected {erlang_version}, found $V)"
        exit 1
    fi

    {test_env}

    # /usr/local/bin/tree

    {erlang_home}/bin/ct_run \\
        -no_auto_compile \\
        -noinput \\
        {pa_args} \\
        -dir {suite_beam_dir} \\
        -logdir ${{TEST_UNDECLARED_OUTPUTS_DIR}} \\
        -sname ct-{project}-{name}
    """.format(
        begins_with_fun=BEGINS_WITH_FUN,
        query_erlang_version=QUERY_ERL_VERSION,
        erlang_home=ctx.attr._erlang_home[ErlangHomeProvider].path,
        erlang_version=ctx.attr.erlang_version,
        pa_args=pa_args,
        suite_beam_dir=path_join(erlang_lib_info.lib_dir.short_path, "ebin"),
        project=erlang_lib_info.lib_name,
        name=sanitize_sname(ctx.label.name),
        test_env=" && ".join(test_env_commands)
    )

    script_file = ctx.actions.declare_file(ctx.attr.name + ".sh")

    ctx.actions.write(
        output = script_file,
        content = script,
    )

    lib_dirs = [info.lib_dir for info in [erlang_lib_info] + [dep[ErlangLibInfo] for dep in ctx.attr.deps + ctx.attr.runtime_deps]]

    runfiles = ctx.runfiles(files = lib_dirs)
    for tool in ctx.attr.tools:
        runfiles = runfiles.merge(tool[DefaultInfo].default_runfiles)

    return [DefaultInfo(
        executable = script_file,
        runfiles = runfiles,
    )]

ct_test = rule(
    implementation = _impl,
    attrs = {
        "_erlang_home": attr.label(default = ":erlang_home"),
        "app_name": attr.string(mandatory=True),
        "app_version": attr.string(default="0.1.0"),
        "suites": attr.label_list(allow_files=[".erl"]),
        "hdrs": attr.label_list(allow_files=[".hrl"]),
        "priv": attr.label_list(allow_files = True), # This should be removed once compilation is pulled out of bazel_erlang_lib
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "runtime_deps": attr.label_list(providers=[ErlangLibInfo]),
        "tools": attr.label_list(),
        "erlc_opts": attr.string_list(),
        "erlang_version": attr.string(),
        "test_env": attr.string_dict(),
    },
    test = True,
)