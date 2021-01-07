load(":bazel_erlang_lib.bzl", "ErlangLibInfo")

def _strip_lib_path(f):
    if f.basename.endswith(".beam") or f.basename.endswith(".app"):
        parts = f.path.rpartition("ebin/")
        return "{dst}={src}".format(
            src=f.path,
            dst="ebin/" + parts[2],
        )
    else:
        return None

def _impl(ctx):
    lib_info = ctx.attr.lib[ErlangLibInfo]

    ez_file = ctx.actions.declare_file("{}-{}.ez".format(lib_info.lib_name, lib_info.lib_version))
    zipper_args = ctx.actions.args()
    # TODO: the beam files should have no compression
    zipper_args.add("c", ez_file)
    zipper_args.add_all([lib_info.lib_dir], map_each=_strip_lib_path)
    ctx.actions.run(
        inputs = [lib_info.lib_dir],
        outputs = [ez_file],
        executable = ctx.executable._zipper,
        arguments = [zipper_args],
        progress_message = "Zipping erlang files...",
        mnemonic = "zipper",
    )

    return [DefaultInfo(files = depset([ez_file]))]

ez = rule(
    implementation = _impl,
    attrs = {
        "lib": attr.label(providers=[ErlangLibInfo]),
        "_zipper": attr.label(default = Label("@bazel_tools//tools/zip:zipper"), cfg = "host", executable=True),
    },
)