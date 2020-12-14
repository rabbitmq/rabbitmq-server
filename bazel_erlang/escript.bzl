load(":bazel_erlang_lib.bzl", "ErlangLibInfo")

def _impl(ctx):
    header = ctx.actions.declare_file("header")
    ctx.actions.write(
        output = header,
        content = """#!/usr/bin/env escript
%% This is an -*- erlang -*- file
%%! -escript main {NAME}
""".format(NAME=ctx.label.name)
    )

    beam_files = depset(transitive = [dep[ErlangLibInfo].beam_files for dep in ctx.attr.deps])

    escript_zip = ctx.actions.declare_file("escript.zip")
    zipper_args = ctx.actions.args()
    zipper_args.add("c", escript_zip)
    # TODO: try the flatten arg (f) of zipper
    zipper_args.add_all(
        ["{dst}={src}".format(dst=f.basename, src=f.path) for f in beam_files.to_list()]
    )
    ctx.actions.run(
        inputs = beam_files,
        outputs = [escript_zip],
        executable = ctx.executable._zipper,
        arguments = [zipper_args],
        progress_message = "Zipping erlang files...",
        mnemonic = "zipper",
    )

    ctx.actions.run_shell(
        inputs = [header, escript_zip],
        outputs = [ctx.outputs.executable],
        command = "cat {h} {t} >> {out} && chmod +x {out}".format(h=header.path, t=escript_zip.path, out=ctx.outputs.executable.path),
    )

escript = rule(
    implementation = _impl,
    attrs = {
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "_zipper": attr.label(default = Label("@bazel_tools//tools/zip:zipper"), cfg = "host", executable=True),
    },
    executable = True,
)