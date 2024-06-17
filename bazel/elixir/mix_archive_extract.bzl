load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
    "flat_deps",
)
load(
    "@rules_erlang//:util.bzl",
    "path_join",
)

def _impl(ctx):
    ebin = ctx.actions.declare_directory(path_join(ctx.attr.app_name, "ebin"))

    script = """set -euo pipefail

DEST="$(mktemp -d)"
unzip -q -d "$DEST" {archive}
cp "$DEST"/{app_name}/ebin/* {ebin}
""".format(
    archive = ctx.file.archive.path,
    app_name = ctx.attr.app_name,
    ebin = ebin.path,
)

    ctx.actions.run_shell(
        inputs = ctx.files.archive,
        outputs = [ebin],
        command = script,
        mnemonic = "MixArchiveExtract",
    )

    deps = flat_deps(ctx.attr.deps)

    runfiles = ctx.runfiles([ebin])
    for dep in ctx.attr.deps:
        runfiles = runfiles.merge(dep[DefaultInfo].default_runfiles)

    return [
        DefaultInfo(
            files = depset([ebin]),
            runfiles = runfiles,
        ),
        ErlangAppInfo(
            app_name = ctx.attr.app_name,
            extra_apps = ctx.attr.extra_apps,
            include = [],
            beam = [ebin],
            priv = [],
            license_files = [],
            srcs = ctx.files.srcs,
            deps = deps,
        )
    ]

mix_archive_extract = rule(
    implementation = _impl,
    attrs = {
        "app_name": attr.string(mandatory = True),
        "extra_apps": attr.string_list(),
        "deps": attr.label_list(providers = [ErlangAppInfo]),
        "archive": attr.label(
            allow_single_file = [".ez"],
        ),
        "srcs": attr.label_list(),
    },
    provides = [ErlangAppInfo],
)
