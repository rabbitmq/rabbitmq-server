load(":bazel_erlang_lib.bzl", "ErlangLibInfo", "compile_erlang_action")

def pa(dep):
    c = []
    c.append(dep.label.workspace_root) if dep.label.workspace_root != "" else None
    c.append(dep.label.package) if dep.label.package != "" else None
    c.append(dep[ErlangLibInfo].beam_path)
    return "/".join(c)

def suite_beam_dir(package, erlang_lib_info):
    c = []
    c.append(package) if package != "" else None
    c.append(erlang_lib_info.beam_path)
    return "/".join(c)

def _impl(ctx):
    erlang_lib_info = compile_erlang_action(ctx, srcs=ctx.files.suites, hdrs=ctx.files.hdrs)

    script = """exec env HOME=${{TEST_TMPDIR}} ct_run \\
    -no_auto_compile \\
    -noinput \\
    -pa {beam_dirs} \\
    -dir {suite_beam_dir} \\
    -logdir ${{TEST_UNDECLARED_OUTPUTS_DIR}} \\
    -sname ct-{name}
    """.format(
        beam_dirs=" ".join([pa(dep) for dep in ctx.attr.deps]),
        suite_beam_dir=suite_beam_dir(ctx.label.package, erlang_lib_info),
        name=ctx.label.name,
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    suites = ctx.runfiles(files = erlang_lib_info.beam_files.to_list())
    deps_beam_files = depset(transitive=[dep[ErlangLibInfo].beam_files for dep in ctx.attr.deps])
    srcs = ctx.runfiles(files = deps_beam_files.to_list())

    return [DefaultInfo(runfiles = suites.merge(srcs))]

ct_test = rule(
    implementation = _impl,
    attrs = {
        "suites": attr.label_list(allow_files=[".erl"]),
        "hdrs": attr.label_list(allow_files=[".hrl"]),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "erlc_opts": attr.string_list(),
    },
    test = True,
)