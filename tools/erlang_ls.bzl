load(
    "@rules_erlang//tools:erlang_toolchain.bzl",
    "erlang_dirs",
)

def _impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name)

    (erlang_home, _, _) = erlang_dirs(ctx)

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
  - deps/*
  - deps/*/include
  - deps/*/src
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
    toolchains = [
        "@rules_erlang//tools:toolchain_type",
    ],
)
