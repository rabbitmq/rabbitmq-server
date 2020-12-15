load(":erlang_home.bzl", "ErlangHomeProvider")

ErlangLibInfo = provider(
    doc = "Compiled Erlang sources",
    fields = {
        'hdrs': 'Public Headers of the library',
        'beam_files': 'Compiled sources',
        'beam_path': 'the relative add path of the beam files'
    },
)

def unique_dirnames(files):
    dirs = []
    for f in files:
        if f.dirname not in dirs:
            dirs.append(f.dirname)
    return dirs

def declared_beam_file(ctx, directory, f):
    name = f.basename.replace(".erl", ".beam", 1)
    return ctx.actions.declare_file("/".join([directory, name]))

def compile_erlang_action(ctx, srcs=[], hdrs=[]):
    beam_path = "ebin" if not ctx.attr.testonly else "ebin_test"

    outs = [declared_beam_file(ctx, beam_path, f) for f in srcs]

    erl_args = ctx.actions.args()
    erl_args.add("-v")
    # Due to the sandbox, and the lack of any undeclared files, we should be able to just include every dir of every header
    # (after deduplication)
    for dir in unique_dirnames(hdrs):
        erl_args.add("-I", dir)

    # NOTE: the headers of deps should work with `include_lib` calls
    # a cheat for now since we only include_lib for externals
    erl_args.add("-I", "external")
    # another cheat based on the monorepo/erlang.mk layout
    erl_args.add("-I", "deps")
    erl_args.add("-I", ctx.bin_dir.path + "/deps")

    dep_beam_files = depset(transitive = [dep[ErlangLibInfo].beam_files for dep in ctx.attr.deps])
    for dir in unique_dirnames(dep_beam_files.to_list()):
        erl_args.add("-pa", dir)

    erl_args.add("-o", outs[0].dirname)
    if ctx.attr.testonly:
        erl_args.add("-DTEST")

    erl_args.add_all(ctx.attr.erlc_opts)

    erl_args.add_all(srcs)

    dep_hdrs = depset(transitive = [dep[ErlangLibInfo].hdrs for dep in ctx.attr.deps])
    dep_beam_files = depset(transitive = [dep[ErlangLibInfo].beam_files for dep in ctx.attr.deps])

    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path

    # ctx.actions.run(
    #     inputs = srcs + hdrs + dep_beam_files.to_list() + dep_hdrs.to_list(),
    #     outputs = outs,
    #     executable = erlang_home + "/bin/erlc",
    #     arguments = [erl_args],
    # )
    ctx.actions.run_shell(
        inputs = srcs + hdrs + dep_beam_files.to_list() + dep_hdrs.to_list(),
        outputs = outs,
        command = "set -x; find . && " + erlang_home + "/bin/erlc $@",
        arguments = [erl_args]
    )

    return ErlangLibInfo(
        hdrs = depset(direct = hdrs, transitive = [dep_hdrs]),
        beam_files = depset(direct = outs),
        beam_path = beam_path,
    )

def _impl(ctx):
    erlang_lib_info = compile_erlang_action(ctx, srcs=ctx.files.srcs, hdrs=ctx.files.hdrs)

    return [
        DefaultInfo(files = erlang_lib_info.beam_files),
        erlang_lib_info,
    ]

# what we probably want for external libs is an 'rebar3_lib' or 'erlang_mk_lib'
# that understands the config tools. But this may do for now for the sake of
# testing
bazel_erlang_lib = rule(
    implementation = _impl,
    attrs = {
        "hdrs": attr.label_list(allow_files=[".hrl"]),
        "srcs": attr.label_list(allow_files=[".erl"]),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "erlc_opts": attr.string_list(),
        "_erlang_home": attr.label(default = ":erlang_home"),
    },
)
