load("//bazel_erlang:bazel_erlang_lib.bzl", "path_join")

def copy_to_sbin(s, ctx=None):
    d = ctx.actions.declare_file(path_join("sbin", s.basename))
    args = ctx.actions.args()
    args.add_all([s, d])
    ctx.actions.run(
        inputs = [s],
        outputs = [d],
        executable = "cp",
        arguments = [args],
    )
    return d

def copy_to_escript(s, ctx=None):
    d = ctx.actions.declare_file(path_join("escript", s.basename))
    args = ctx.actions.args()
    args.add_all([s, d])
    ctx.actions.run(
        inputs = [s],
        outputs = [d],
        executable = "cp",
        arguments = [args],
    )
    return d

def _impl(ctx):
    outs = []
    executable = None
    for s in ctx.files.scripts:
        if s.basename == ctx.label.name:
            executable = copy_to_sbin(s, ctx=ctx)
        else:
            outs.append(copy_to_sbin(s, ctx=ctx))

    for s in ctx.files.escripts:
        outs.append(copy_to_escript(s, ctx=ctx))

    return DefaultInfo(
        executable = executable,
        runfiles = ctx.runfiles(outs),
    )

rabbitmq_sbin = rule(
    implementation = _impl,
    attrs = {
        "scripts": attr.label_list(allow_files = True),
        "escripts": attr.label_list(),
    },
    executable = True,
)