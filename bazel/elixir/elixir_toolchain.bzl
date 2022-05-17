load(
    "@rules_erlang//private:erlang_build.bzl",
    "OtpInfo",
)
load(
    ":elixir_build.bzl",
    "ElixirInfo",
)

def _impl(ctx):
    toolchain_info = platform_common.ToolchainInfo(
        otpinfo = ctx.attr.elixir[OtpInfo],
        elixirinfo = ctx.attr.elixir[ElixirInfo],
    )
    return [toolchain_info]

elixir_toolchain = rule(
    implementation = _impl,
    attrs = {
        "elixir": attr.label(
            mandatory = True,
            providers = [OtpInfo, ElixirInfo],
        ),
    },
    provides = [platform_common.ToolchainInfo],
)

def _build_info(ctx):
    return ctx.toolchains[":toolchain_type"].otpinfo

def erlang_dirs(ctx):
    info = _build_info(ctx)
    if info.release_dir != None:
        runfiles = ctx.runfiles([
            info.release_dir,
            info.version_file,
        ])
    else:
        runfiles = ctx.runfiles([
            info.version_file,
        ])
    return (info.erlang_home, info.release_dir, runfiles)

def elixir_dirs(ctx, short_path = False):
    info = ctx.toolchains[":toolchain_type"].elixirinfo
    if info.elixir_home != None:
        return (info.elixir_home, ctx.runfiles([info.version_file]))
    else:
        p = info.release_dir.short_path if short_path else info.release_dir.path
        return (p, ctx.runfiles([info.release_dir, info.version_file]))

def maybe_symlink_erlang(ctx, short_path = False):
    info = _build_info(ctx)
    release_dir = info.release_dir
    if release_dir == None:
        return ""
    else:
        return """mkdir -p $(dirname "{erlang_home}")
ln -sf $PWD/{erlang_release_dir} "{erlang_home}"
ERTS_DIRNAME="$(basename "$(echo "{erlang_home}"/erts-*)")"
ln -sf ../$ERTS_DIRNAME/bin/epmd "{erlang_home}"/bin/epmd
""".format(
            erlang_release_dir = release_dir.short_path if short_path else release_dir.path,
            erlang_home = info.erlang_home,
        )
