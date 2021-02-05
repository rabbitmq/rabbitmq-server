load(":erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load(":bazel_erlang_lib.bzl", "ErlangLibInfo",
                              "path_join",
                              "unique_dirnames",
                              "beam_file",
                              "BEGINS_WITH_FUN",
                              "QUERY_ERL_VERSION",
                              "erlc")

def sanitize_sname(s):
    return s.replace("@", "-").replace(".", "_")

def short_dirname(f):
    return f.short_path.rpartition("/")[0]

def unique_short_dirnames(files):
    dirs = []
    for f in files:
        dirname = short_dirname(f)
        if dirname not in dirs:
            dirs.append(dirname)
    return dirs

def _impl(ctx):
    paths = []
    for dep in ctx.attr.deps:
        paths.extend(unique_short_dirnames(dep[ErlangLibInfo].beam))

    pa_args = " ".join(["-pa {}".format(p) for p in paths])

    test_env_commands = []
    for k, v in ctx.attr.test_env.items():
        test_env_commands.append("export {}=\"{}\"".format(k, v))

    sname = sanitize_sname("ct-{}-{}".format(
        ctx.label.package.partition("/")[-1],
        ctx.label.name,
    ))

    script = """set -euxo pipefail

    export HOME=${{TEST_TMPDIR}}

    {begins_with_fun}
    V=$({erlang_home}/bin/{query_erlang_version})
    if ! beginswith "{erlang_version}" "$V"; then
        echo "Erlang version mismatch (Expected {erlang_version}, found $V)"
        exit 1
    fi

    {test_env}

    {erlang_home}/bin/ct_run \\
        -no_auto_compile \\
        -noinput \\
        {pa_args} \\
        -dir {suite_beam_dir} \\
        -logdir ${{TEST_UNDECLARED_OUTPUTS_DIR}} \\
        -sname {sname}
    """.format(
        begins_with_fun=BEGINS_WITH_FUN,
        query_erlang_version=QUERY_ERL_VERSION,
        erlang_home=ctx.attr._erlang_home[ErlangHomeProvider].path,
        erlang_version=ctx.attr._erlang_version[ErlangVersionProvider].version,
        pa_args=pa_args,
        suite_beam_dir=short_dirname(ctx.files.suites[0]),
        sname=sname,
        test_env=" && ".join(test_env_commands)
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(files = ctx.files.suites)
    for dep in ctx.attr.deps:
        runfiles = runfiles.merge(ctx.runfiles(dep[DefaultInfo].files.to_list()))
    for tool in ctx.attr.tools:
        runfiles = runfiles.merge(tool[DefaultInfo].default_runfiles)

    return [DefaultInfo(
        runfiles = runfiles,
    )]

ct_test = rule(
    implementation = _impl,
    attrs = {
        "_erlang_home": attr.label(default = ":erlang_home"),
        "_erlang_version": attr.label(default = ":erlang_version"),
        "suites": attr.label_list(allow_files=[".beam"]),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "tools": attr.label_list(),
        "test_env": attr.string_dict(),
    },
    test = True,
)

def ct_suite(
    suite_name="",
    deps=[],
    runtime_deps=[],
    tools=[],
    test_env={},
    **kwargs):

    erlc(
        name = "{}_beam_files".format(suite_name),
        hdrs = native.glob(["include/*.hrl"]),
        srcs = ["test/{}.erl".format(suite_name)],
        dest = "test",
        deps = [":test_bazel_erlang_lib"] + deps,
    )

    ct_test(
        name = suite_name,
        suites = [":{}_beam_files".format(suite_name)],
        deps = [":test_bazel_erlang_lib"] + deps + runtime_deps,
        tools = tools,
        test_env = test_env,
        **kwargs
    )