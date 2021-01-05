load(":bazel_erlang_lib.bzl", "ErlangLibInfo", "compile_erlang_action")

def _impl(ctx):
    erlang_lib_info0 = compile_erlang_action(ctx, srcs=ctx.files.srcs, hdrs=ctx.files.hdrs)

    app_file = ctx.actions.declare_file("{}/{}.app".format(erlang_lib_info0.beam_path, ctx.attr.app_name))

    # TODO: have different version of this rule that work with
    #       src/app_name.app.src if present OR rebar OR erlang.mk
    ctx.actions.write(
        output = app_file,
        content = """{{application,{name},[
            {{vsn, "{version}"}}
        ]}}.""".format(name=ctx.attr.app_name, version=ctx.attr.app_version)
    )

    erlang_lib_info1 = ErlangLibInfo(
        name = ctx.attr.app_name,
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
        "app_version": attr.string(mandatory=True),
        "hdrs": attr.label_list(allow_files=[".hrl"]),
        "srcs": attr.label_list(allow_files=[".erl"]),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "erlc_opts": attr.string_list(),
        "_erlang_home": attr.label(default = ":erlang_home"),
    },
)
