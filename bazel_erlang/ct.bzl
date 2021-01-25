load(":bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join", "compile_erlang_action")

def lib_dir(dep):
    c = []
    c.append(dep.label.workspace_root) if dep.label.workspace_root != "" else None
    c.append(dep[ErlangLibInfo].lib_dir.short_path)
    return path_join(*c)

def ebin_dir(dep):
    return path_join(lib_dir(dep), "ebin")

def _impl(ctx):
    erlang_lib_info = compile_erlang_action(
        ctx, 
        srcs=ctx.files.suites, 
        hdrs=ctx.files.hdrs,
        gen_app_file=False,
    )

    pa_args = " ".join(
        ["-pa {}".format(ebin_dir(dep)) for dep in ctx.attr.deps]
    )

    script = """set -euo pipefail
    # pwd
    # /usr/local/bin/tree
    exec env HOME=${{TEST_TMPDIR}} ct_run \\
        -no_auto_compile \\
        -noinput \\
        {pa_args} \\
        -dir {suite_beam_dir} \\
        -logdir ${{TEST_UNDECLARED_OUTPUTS_DIR}} \\
        -sname ct-{project}-{name}
    """.format(
        pa_args=pa_args,
        suite_beam_dir=path_join(erlang_lib_info.lib_dir.short_path, "ebin"),
        project=erlang_lib_info.lib_name,
        name=ctx.label.name,
    )

    script_file = ctx.actions.declare_file(ctx.attr.name + ".sh")

    ctx.actions.write(
        output = script_file,
        content = script,
    )

    lib_dirs = [info.lib_dir for info in [erlang_lib_info] + [dep[ErlangLibInfo] for dep in ctx.attr.deps]]

    return [DefaultInfo(
        executable = script_file,
        runfiles = ctx.runfiles(files = lib_dirs),
    )]

ct_test = rule(
    implementation = _impl,
    attrs = {
        "app_name": attr.string(mandatory=True),
        "app_version": attr.string(default="0.1.0"),
        "suites": attr.label_list(allow_files=[".erl"]),
        "hdrs": attr.label_list(allow_files=[".hrl"]),
        "priv": attr.label_list(allow_files = True), # This should be removed once compilation is pulled out of bazel_erlang_lib
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "runtime_deps": attr.label_list(providers=[ErlangLibInfo]),
        "erlc_opts": attr.string_list(),
        "_erlang_version": attr.label(default = ":erlang_version"),
        "_erlang_home": attr.label(default = ":erlang_home"),
    },
    test = True,
)