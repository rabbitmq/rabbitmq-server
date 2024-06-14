load("@bazel_skylib//lib:shell.bzl", "shell")
load("@rules_erlang//:erlang_app_info.bzl", "ErlangAppInfo", "flat_deps")
load("@rules_erlang//:util.bzl", "path_join")
load("@rules_erlang//private:util.bzl", "erl_libs_contents")

def _impl(ctx):
    ebin = ctx.actions.declare_directory(ctx.attr.dest)

    erl_libs_dir = ctx.label.name + "_deps"

    erl_libs_files = erl_libs_contents(
        ctx,
        target_info = None,
        headers = True,
        dir = erl_libs_dir,
        deps = flat_deps(ctx.attr.deps),
        ez_deps = ctx.files.ez_deps,
        expand_ezs = True,
    )

    erl_libs_path = ""
    if len(erl_libs_files) > 0:
        erl_libs_path = path_join(
            ctx.bin_dir.path,
            ctx.label.workspace_root,
            ctx.label.package,
            erl_libs_dir,
        )

    env = "\n".join([
        "export {}={}".format(k, v)
        for k, v in ctx.attr.env.items()
    ])

    script = """set -euo pipefail

if [ -n "{erl_libs_path}" ]; then
    export ERL_LIBS={erl_libs_path}
fi

{env}

{setup}
set -x
{elixirc} \\
    -o {out_dir} \\
    {elixirc_opts} \\
    {srcs}
""".format(
        erl_libs_path = erl_libs_path,
        env = env,
        setup = ctx.attr.setup,
        elixirc = ctx.executable._compiler.path,
        out_dir = ebin.path,
        elixirc_opts = " ".join([shell.quote(opt) for opt in ctx.attr.elixirc_opts]),
        srcs = " ".join([f.path for f in ctx.files.srcs]),
    )

    compiler_runfiles = ctx.attr._compiler[DefaultInfo].default_runfiles

    inputs = depset(
        direct = ctx.files.srcs + erl_libs_files,
        transitive = [
            compiler_runfiles.files,
        ],
    )

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [ebin],
        command = script,
        mnemonic = "ELIXIRC",
        tools = [ctx.executable._compiler],
    )

    return [
        DefaultInfo(
            files = depset([ebin]),
        )
    ]

elixir_bytecode = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".ex"],
        ),
        "elixirc_opts": attr.string_list(),
        "env": attr.string_dict(),
        "deps": attr.label_list(
            providers = [ErlangAppInfo],
        ),
        "ez_deps": attr.label_list(
            allow_files = [".ez"],
        ),
        "dest": attr.string(
            mandatory = True,
        ),
        "setup": attr.string(),
        "_compiler": attr.label(
            default = Label("@rules_elixir//tools:elixirc"),
            allow_single_file = True,
            executable = True,
            cfg = "exec",
        ),
    },
    # toolchains = ["@rules_elixir//:toolchain_type"],
)
