load("@bazel-erlang//:erlang_home.bzl", "ErlangHomeProvider")

def _impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name)

    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path

    ctx.actions.write(
        output = out,
        content = """otp_path: {erlang_home}
apps_dirs:
  - deps/*
  - deps/rabbit/apps/*
deps_dirs:
  - bazel-bin/external/*
include_dirs:
  - deps
  - deps/*/include
  - bazel-bin/external
  - bazel-bin/external/*/include
plt_path: bazel-bin/deps/rabbit/.base_plt.plt
""".format(
            erlang_home = erlang_home,
        ),
    )

    return [
        DefaultInfo(files = depset([out])),
    ]

erlang_ls_config = rule(
    implementation = _impl,
    attrs = {
        "_erlang_home": attr.label(default = "@bazel-erlang//:erlang_home"),
    },
)
