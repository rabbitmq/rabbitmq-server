load(":bazel_erlang_lib.bzl", "ErlangLibInfo", "compile_erlang_action")

def _impl(ctx):
    erlang_lib_info0 = compile_erlang_action(ctx, srcs=ctx.files.srcs, hdrs=ctx.files.hdrs)

    app_file = ctx.actions.declare_file("{}/{}.app".format(erlang_lib_info0.beam_path, ctx.attr.app_name))

    ctx.actions.write(
        output = app_file,
        content = "{{application, '{}', []}}".format(ctx.attr.app_name)
    )

    erlang_lib_info1 = ErlangLibInfo(
        hdrs = erlang_lib_info0.hdrs,
        beam_files = depset(direct = [app_file] + erlang_lib_info0.beam_files.to_list()),
        beam_path = erlang_lib_info0.beam_path,
    )

    return [
        DefaultInfo(files = erlang_lib_info1.beam_files),
        erlang_lib_info1,
    ]

# This behaves very much like bazel_erlang_lib, but also generates the '.app' file
bazel_erlang_app = rule(
    implementation = _impl,
    attrs = {
        "app_name": attr.string(mandatory=True),
        "hdrs": attr.label_list(allow_files=[".hrl"]),
        "srcs": attr.label_list(allow_files=[".erl"]),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "erlc_opts": attr.string_list(),
        "_erlang_home": attr.label(default = ":erlang_home"),
    },
)
