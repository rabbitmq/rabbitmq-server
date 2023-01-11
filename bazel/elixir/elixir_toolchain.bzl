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
    if info.release_dir_tar != None:
        runfiles = ctx.runfiles([
            info.release_dir_tar,
            info.version_file,
        ])
    else:
        runfiles = ctx.runfiles([
            info.version_file,
        ])
    return (info.erlang_home, info.release_dir_tar, runfiles)

def elixir_dirs(ctx, short_path = False):
    info = ctx.toolchains[":toolchain_type"].elixirinfo
    if info.elixir_home != None:
        return (info.elixir_home, ctx.runfiles([info.version_file]))
    else:
        p = info.release_dir.short_path if short_path else info.release_dir.path
        return (p, ctx.runfiles([info.release_dir, info.version_file]))

def maybe_install_erlang(ctx, short_path = False):
    info = _build_info(ctx)
    release_dir_tar = info.release_dir_tar
    if release_dir_tar == None:
        return ""
    else:
        return """\
tar --extract \\
    --directory / \\
    --file {release_tar}""".format(
            release_tar = release_dir_tar.short_path if short_path else release_dir_tar.path,
            erlang_home = info.erlang_home,
        )
