load(":erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")
load(":bazel_erlang_lib.bzl", "ErlangLibInfo",
                              "path_join",
                              "unique_dirnames",
                              "beam_file",
                              "BEGINS_WITH_FUN",
                              "QUERY_ERL_VERSION")

def lib_dir(dep):
    c = []
    c.append(dep.label.workspace_root) if dep.label.workspace_root != "" else None
    c.append(dep[ErlangLibInfo].lib_dir.short_path)
    return path_join(*c)

def sanitize_sname(s):
    return s.replace("@", "-").replace(".", "_")

def short_dirname(f):
    parts = f.short_path.partition("/")
    return path_join(*(parts[0:-2]))

def _compile_srcs(ctx):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version

    # Note: the sources must be placed in the src dir, so as not to conflict
    #       with the files compiled without test compiler options
    beam_files = [beam_file(ctx, src, "src") for src in ctx.files.srcs]

    ebin_dir = beam_files[0].dirname

    erl_args = ctx.actions.args()
    erl_args.add("-v")

    for dir in unique_dirnames(ctx.files.hdrs):
        erl_args.add("-I", dir)

    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        if lib_info.erlang_version != erlang_version:
            fail("Mismatched erlang versions", erlang_version, lib_info.erlang_version)
        for dir in unique_dirnames(lib_info.include):
            erl_args.add("-I", path_join(dir, "../.."))
        for dir in unique_dirnames(lib_info.beam):
            erl_args.add("-pa", dir)

    erl_args.add("-o", ebin_dir)

    erl_args.add("-DTEST")
    erl_args.add("+debug_info")
    erl_args.add_all(ctx.attr.erlc_opts)

    erl_args.add_all(ctx.files.srcs)

    script = """
        set -euo pipefail

        # /usr/local/bin/tree $PWD

        mkdir -p {ebin_dir}
        export HOME=$PWD

        {begins_with_fun}
        V=$({erlang_home}/bin/{query_erlang_version})
        if ! beginswith "{erlang_version}" "$V"; then
            echo "Erlang version mismatch (Expected {erlang_version}, found $V)"
            exit 1
        fi

        {erlang_home}/bin/erlc $@
    """.format(
        ebin_dir=ebin_dir,
        begins_with_fun=BEGINS_WITH_FUN,
        query_erlang_version=QUERY_ERL_VERSION,
        erlang_version=erlang_version,
        erlang_home=ctx.attr._erlang_home[ErlangHomeProvider].path,
    )

    inputs = []
    inputs.extend(ctx.files.hdrs)
    inputs.extend(ctx.files.srcs)
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        inputs.extend(lib_info.include)
        inputs.extend(lib_info.beam)

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = beam_files,
        command = script,
        arguments = [erl_args],
        env = {
            # "ERLANG_VERSION": ctx.attr.erlang_version,
        },
        mnemonic = "ERLC",
    )

    return beam_files

def _compile_suites(ctx, srcs_beam_files):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version

    beam_files = [beam_file(ctx, src, "test") for src in ctx.files.suites]

    test_dir = beam_files[0].dirname

    erl_args = ctx.actions.args()
    erl_args.add("-v")

    for dir in unique_dirnames(ctx.files.hdrs):
        erl_args.add("-I", dir)

    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        if lib_info.erlang_version != erlang_version:
            fail("Mismatched erlang versions", erlang_version, lib_info.erlang_version)
        for dir in unique_dirnames(lib_info.include):
            erl_args.add("-I", path_join(dir, "../.."))
        for dir in unique_dirnames(lib_info.beam):
            erl_args.add("-pa", dir)

    for dir in unique_dirnames(srcs_beam_files):
        erl_args.add("-pa", dir)

    erl_args.add("-o", test_dir)

    erl_args.add("-DTEST")
    erl_args.add("+debug_info")
    erl_args.add_all(ctx.attr.erlc_opts)

    erl_args.add_all(ctx.files.suites)

    script = """
        set -euo pipefail

        # /usr/local/bin/tree $PWD

        mkdir -p {test_dir}
        export HOME=$PWD

        {begins_with_fun}
        V=$({erlang_home}/bin/{query_erlang_version})
        if ! beginswith "{erlang_version}" "$V"; then
            echo "Erlang version mismatch (Expected {erlang_version}, found $V)"
            exit 1
        fi

        {erlang_home}/bin/erlc $@
    """.format(
        test_dir=test_dir,
        begins_with_fun=BEGINS_WITH_FUN,
        query_erlang_version=QUERY_ERL_VERSION,
        erlang_version=erlang_version,
        erlang_home=ctx.attr._erlang_home[ErlangHomeProvider].path,
    )

    inputs = []
    inputs.extend(ctx.files.hdrs)
    inputs.extend(ctx.files.suites)
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        inputs.extend(lib_info.include)
        inputs.extend(lib_info.beam)

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = beam_files,
        command = script,
        arguments = [erl_args],
        env = {
            # "ERLANG_VERSION": ctx.attr.erlang_version,
        },
        mnemonic = "ERLC",
    )

    return beam_files

def _impl(ctx):
    srcs_beam_files = _compile_srcs(ctx)
    suite_beam_files = _compile_suites(ctx, srcs_beam_files)

    paths = []
    paths.append(short_dirname(srcs_beam_files[0]))
    for dep in ctx.attr.deps + ctx.attr.runtime_deps:
        paths.append(short_dirname(dep[ErlangLibInfo].beam[0]))

    pa_args = " ".join(["-pa {}".format(p) for p in paths])

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

    # /usr/local/bin/tree $PWD

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
        erlang_version=ctx.attr._erlang_version[ErlangVersionProvider].version,
        pa_args=pa_args,
        suite_beam_dir=short_dirname(suite_beam_files[0]),
        project=ctx.attr.app_name,
        name=sanitize_sname(ctx.label.name),
        test_env=" && ".join(test_env_commands)
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(files = srcs_beam_files + suite_beam_files)
    for dep in ctx.attr.deps:
        runfiles = runfiles.merge(ctx.runfiles(dep[DefaultInfo].files.to_list()))
    for tool in ctx.attr.tools:
        runfiles = runfiles.merge(tool[DefaultInfo].default_runfiles)

    return [DefaultInfo(
        # executable = script_file,
        runfiles = runfiles,
    )]

ct_test = rule(
    implementation = _impl,
    attrs = {
        "_erlang_home": attr.label(default = ":erlang_home"),
        "_erlang_version": attr.label(default = ":erlang_version"),
        "app_name": attr.string(mandatory=True),
        "app_version": attr.string(default="0.1.0"),
        "hdrs": attr.label_list(allow_files=[".hrl"]),
        "srcs": attr.label_list(allow_files=[".erl"]),
        "suites": attr.label_list(allow_files=[".erl"]),
        # "priv": attr.label_list(allow_files = True), # This should be removed once compilation is pulled out of bazel_erlang_lib
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "runtime_deps": attr.label_list(providers=[ErlangLibInfo]),
        "tools": attr.label_list(),
        "erlc_opts": attr.string_list(),
        "test_env": attr.string_dict(),
    },
    test = True,
)